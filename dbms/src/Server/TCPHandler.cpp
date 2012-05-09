#include <iomanip>

#include <boost/bind.hpp>

#include <Yandex/Revision.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>

#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/copyData.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/executeQuery.h>

#include "TCPHandler.h"


namespace DB
{


void TCPHandler::runImpl()
{
	ReadBufferFromPocoSocket in(socket());
	WriteBufferFromPocoSocket out(socket());
	WriteBufferFromPocoSocket out_for_chunks(socket());
	
	/// Сразу после соединения, отправляем hello-пакет.
	sendHello(out);

	while (!in.eof())
	{
		Stopwatch watch;
		state.reset();
		
		try
		{	
			/// Пакет с запросом.
			receivePacket(in, out);

			after_check_cancelled.restart();
			after_send_progress.restart();

			LOG_DEBUG(log, "Query ID: " << state.query_id);
			LOG_DEBUG(log, "Query: " << state.query);
			LOG_DEBUG(log, "In format: " << state.in_format);
			LOG_DEBUG(log, "Out format: " << state.out_format);

			/// Читаем из сети данные для INSERT-а, если надо, и вставляем их.
			if (state.io.out)
			{
				while (receivePacket(in, out))
					;
			}

			/// Вынимаем результат выполнения запроса, если есть, и пишем его в сеть.
			if (state.io.in)
			{
				if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*state.io.in))
				{
					profiling_in->setIsCancelledCallback(boost::bind(&TCPHandler::isQueryCancelled, this, boost::ref(in)));
					profiling_in->setProgressCallback(boost::bind(&TCPHandler::sendProgress, this, boost::ref(out), 0, 0));
				}

				while (sendData(out, out_for_chunks))
					;
			}
			else
				sendOk(out);
		}
		catch (DB::Exception & e)
		{
			LOG_ERROR(log, "DB::Exception. Code: " << e.code() << ", e.displayText() = " << e.displayText()
				<< ", Stack trace:\n\n" << e.getStackTrace().toString());
			state.exception = e.clone();
		}
		catch (Poco::Exception & e)
		{
			LOG_ERROR(log, "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code() << ", e.displayText() = " << e.displayText());
			state.exception = new Exception(e.message(), e.code());
		}
		catch (std::exception & e)
		{
			LOG_ERROR(log, "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", e.what() = " << e.what());
			state.exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
		}
		catch (...)
		{
			LOG_ERROR(log, "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION);
			state.exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
		}

		if (state.exception)
			sendException(out);

		watch.stop();

		LOG_INFO(log, std::fixed << std::setprecision(3)
			<< "Processed in " << watch.elapsedSeconds() << " sec.");
	}
}


void TCPHandler::sendHello(WriteBuffer & out)
{
	writeVarUInt(Protocol::Server::Hello, out);
	writeStringBinary(DBMS_NAME, out);
	writeVarUInt(DBMS_VERSION_MAJOR, out);
	writeVarUInt(DBMS_VERSION_MINOR, out);
	writeVarUInt(Revision::get(), out);
	out.next();
}

bool TCPHandler::receivePacket(ReadBuffer & in, WriteBuffer & out)
{
	while (true)	/// Если пришёл пакет типа Ping, то игнорируем его и получаем следующий пакет.
	{
		UInt64 packet_type = 0;
		readVarUInt(packet_type, in);

		std::cerr << "Packet: " << packet_type << std::endl;

		switch (packet_type)
		{
			case Protocol::Client::Query:
				if (!state.empty())
					throw Exception("Unexpected packet Query received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
				receiveQuery(in);
				return true;

			case Protocol::Client::Data:
				if (state.empty())
					throw Exception("Unexpected packet Data received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
				return receiveData(in);

			case Protocol::Client::Ping:
				writeVarUInt(Protocol::Server::Pong, out);
				out.next();
				break;

			default:
				throw Exception("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
		}
	}
}

void TCPHandler::receiveQuery(ReadBuffer & in)
{
	UInt64 stage = 0;
	UInt64 compression = 0;
	
	readIntBinary(state.query_id, in);

	readVarUInt(stage, in);
	state.stage = QueryProcessingStage::Enum(stage);

	readVarUInt(compression, in);
	state.compression = Protocol::Compression::Enum(compression);

	readStringBinary(state.in_format, in);
	readStringBinary(state.out_format, in);
	
	readStringBinary(state.query, in);

	state.context = server.global_context;
	state.io = executeQuery(state.query, state.context);
}

bool TCPHandler::receiveData(ReadBuffer & in)
{
	if (!state.block_in)
	{
		state.chunked_in = new ChunkedReadBuffer(in, state.query_id);
		state.maybe_compressed_in = state.compression == Protocol::Compression::Enable
			? new CompressedReadBuffer(*state.chunked_in)
			: state.chunked_in;

		state.block_in = state.context.format_factory->getInput(
			state.out_format,
			*state.maybe_compressed_in,
			state.io.out_sample,
			state.context.settings.max_block_size,
			*state.context.data_type_factory);
	}
	
	/// Прочитать из сети один блок и засунуть его в state.io.out (данные для INSERT-а)
	Block block = state.block_in->read();
	if (block)
	{
		state.io.out->write(block);
		return true;
	}
	else
		return false;
}

bool TCPHandler::isQueryCancelled(ReadBufferFromPocoSocket & in)
{
	Poco::ScopedLock<Poco::FastMutex> lock(is_cancelled_mutex);
	
	if (after_check_cancelled.elapsed() / 1000 < state.context.settings.interactive_delay)
		return false;

	after_check_cancelled.restart();
	
	/// Во время выполнения запроса, единственный пакет, который может прийти от клиента - это остановка выполнения запроса.
	std::cerr << "checking cancelled" << std::endl;
	if (in.poll(0))
	{
		std::cerr << "checking cancelled; socket has data" << std::endl;
		
		UInt64 packet_type = 0;
		readVarUInt(packet_type, in);

		switch (packet_type)
		{
			case Protocol::Client::Cancel:
				if (state.empty())
					throw Exception("Unexpected packet Cancel received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
				LOG_INFO(log, "Query was cancelled.");
				return true;

			default:
				throw Exception("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
		}
	}

	return false;
}

bool TCPHandler::sendData(WriteBuffer & out, WriteBuffer & out_for_chunks)
{
	/// Получить один блок результата выполнения запроса из state.io.in.
	Block block = state.io.in->read();

	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);
	
	writeVarUInt(Protocol::Server::Data, out);
	out.next();

	if (!state.block_out)
	{
		std::cerr << "query_id: " << state.query_id << std::endl;
		
		state.chunked_out = new ChunkedWriteBuffer(out_for_chunks, state.query_id);
		state.maybe_compressed_out = state.compression == Protocol::Compression::Enable
			? new CompressedWriteBuffer(*state.chunked_out)
			: state.chunked_out;

		state.block_out = state.context.format_factory->getOutput(
			state.in_format,
			*state.maybe_compressed_out,
			state.io.in_sample);
	}

	/// Если блок есть - записать его в сеть. Иначе записать в сеть признак конца данных.
	if (block)
	{
		state.block_out->write(block);
		state.maybe_compressed_out->next();
		state.chunked_out->next();
		out_for_chunks.next();
		return true;
	}
	else
	{
		dynamic_cast<ChunkedWriteBuffer &>(*state.chunked_out).finish();
		out_for_chunks.next();
		state.sent_all_data = true;
		return false;
	}
}

void TCPHandler::sendException(WriteBuffer & out)
{
	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);
	
	writeVarUInt(Protocol::Server::Exception, out);
	writeException(*state.exception, out);
	out.next();
}

void TCPHandler::sendOk(WriteBuffer & out)
{
	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);

	writeVarUInt(Protocol::Server::Ok, out);
	out.next();
}

void TCPHandler::sendProgress(WriteBuffer & out, size_t rows, size_t bytes)
{
	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);

	/// Не будем отправлять прогресс после того, как отправлены все данные.
	if (state.sent_all_data)
		return;

	if (after_send_progress.elapsed() / 1000 < state.context.settings.interactive_delay)
		return;

	after_send_progress.restart();
	
	writeVarUInt(Protocol::Server::Progress, out);
	writeVarUInt(rows, out);
	writeVarUInt(bytes, out);
	out.next();
}


void TCPHandler::run()
{
	try
	{
		runImpl();

		LOG_INFO(log, "Done processing connection.");
	}
	catch (Poco::Exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.message() = " << e.message() << ", e.what() = " << e.what();
		LOG_ERROR(log, s.str());
	}
	catch (std::exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what();
		LOG_ERROR(log, s.str());
	}
	catch (...)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ". Unknown exception.";
		LOG_ERROR(log, s.str());
	}
}


}
