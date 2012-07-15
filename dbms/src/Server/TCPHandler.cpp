#include <iomanip>

#include <boost/bind.hpp>

#include <Yandex/Revision.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Progress.h>

#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/copyData.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/executeQuery.h>

#include "TCPHandler.h"
#include <Poco/Ext/ThreadNumber.h>


namespace DB
{


void TCPHandler::runImpl()
{
	connection_context = server.global_context;
	connection_context.session_context = &connection_context;
	
	in = new ReadBufferFromPocoSocket(socket());
	out = new WriteBufferFromPocoSocket(socket());
	
	receiveHello();

	/// При соединении может быть указана БД по-умолчанию.
	if (!default_database.empty())
	{
		Poco::ScopedLock<Poco::Mutex> lock(*connection_context.mutex);

		if (connection_context.databases->end() == connection_context.databases->find(default_database))
		{
			Exception e("Database " + default_database + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
			LOG_ERROR(log, "DB::Exception. Code: " << e.code() << ", e.displayText() = " << e.displayText()
				<< ", Stack trace:\n\n" << e.getStackTrace().toString());
			sendException(e);
			return;
		}

		connection_context.current_database = default_database;
	}
	
	sendHello();

	while (!in->eof())
	{
		Stopwatch watch;
		state.reset();
		
		try
		{	
			/// Пакет Query. (Также, если пришёл пакет Ping - обрабатываем его и продолжаем ждать Query.)
			receivePacket();

			/// Обрабатываем Query
			
			after_check_cancelled.restart();
			after_send_progress.restart();

			LOG_DEBUG(log, "Query ID: " << state.query_id);
			LOG_DEBUG(log, "Query: " << state.query);
			LOG_DEBUG(log, "Requested stage: " << QueryProcessingStage::toString(state.stage));

			/// Запрос требует приёма данных от клиента?
			if (state.io.out)
				processInsertQuery();
			else
				processOrdinaryQuery();

			sendEndOfStream();
		}
		catch (DB::Exception & e)
		{
			LOG_ERROR(log, "DB::Exception. Code: " << e.code() << ", e.displayText() = " << e.displayText()
				<< ", Stack trace:\n\n" << e.getStackTrace().toString());
			state.exception = e.clone();

			if (e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT)
				throw;
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
			sendException(*state.exception);

		state.reset();
		watch.stop();

		LOG_INFO(log, std::fixed << std::setprecision(3)
			<< "Processed in " << watch.elapsedSeconds() << " sec.");
	}
}


void TCPHandler::processInsertQuery()
{
	/// Отправляем клиенту блок - структура таблицы.
	Block block = state.io.out_sample;
	sendData(block);

	while (receivePacket())
		;
}


void TCPHandler::processOrdinaryQuery()
{
	/// Вынимаем результат выполнения запроса, если есть, и пишем его в сеть.
	if (state.io.in)
	{
		if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*state.io.in))
		{
			profiling_in->setIsCancelledCallback(boost::bind(&TCPHandler::isQueryCancelled, this));
			profiling_in->setProgressCallback(boost::bind(&TCPHandler::sendProgress, this, _1, _2));

			std::stringstream query_pipeline;
			profiling_in->dumpTree(query_pipeline);
			LOG_DEBUG(log, "Query pipeline:\n" << query_pipeline.rdbuf());
		}

		while (true)
		{
			Block block = state.io.in->read();
			sendData(block);
			if (!block)
				break;
		}
	}
}


void TCPHandler::receiveHello()
{
	/// Получить hello пакет.
	UInt64 packet_type = 0;
	String client_name;
	UInt64 client_version_major = 0;
	UInt64 client_version_minor = 0;
	UInt64 client_revision = 0;

	readVarUInt(packet_type, *in);
	if (packet_type != Protocol::Client::Hello)
		throw Exception("Unexpected packet from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

	readStringBinary(client_name, *in);
	readVarUInt(client_version_major, *in);
	readVarUInt(client_version_minor, *in);
	readVarUInt(client_revision, *in);
	readStringBinary(default_database, *in);

	LOG_DEBUG(log, "Connected " << client_name
		<< " version " << client_version_major
		<< "." << client_version_minor
		<< "." << client_revision
		<< (!default_database.empty() ? ", database: " + default_database : "")
		<< ".")
}


void TCPHandler::sendHello()
{
	writeVarUInt(Protocol::Server::Hello, *out);
	writeStringBinary(DBMS_NAME, *out);
	writeVarUInt(DBMS_VERSION_MAJOR, *out);
	writeVarUInt(DBMS_VERSION_MINOR, *out);
	writeVarUInt(Revision::get(), *out);
	out->next();
}


bool TCPHandler::receivePacket()
{
	while (true)	/// Если пришёл пакет типа Ping, то обработаем его и получаем следующий пакет.
	{
		UInt64 packet_type = 0;
		readVarUInt(packet_type, *in);

	//	std::cerr << "Packet: " << packet_type << std::endl;

		switch (packet_type)
		{
			case Protocol::Client::Query:
				if (!state.empty())
					throw Exception("Unexpected packet Query received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
				receiveQuery();
				return true;

			case Protocol::Client::Data:
				if (state.empty())
					throw Exception("Unexpected packet Data received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
				return receiveData();

			case Protocol::Client::Ping:
				writeVarUInt(Protocol::Server::Pong, *out);
				out->next();
				break;

			default:
				throw Exception("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
		}
	}
}


void TCPHandler::receiveQuery()
{
	UInt64 stage = 0;
	UInt64 compression = 0;
	
	readIntBinary(state.query_id, *in);

	readVarUInt(stage, *in);
	state.stage = QueryProcessingStage::Enum(stage);

	readVarUInt(compression, *in);
	state.compression = Protocol::Compression::Enum(compression);

	readStringBinary(state.query, *in);

	state.context = connection_context;
	state.io = executeQuery(state.query, state.context, state.stage);
}


bool TCPHandler::receiveData()
{
	if (!state.block_in)
	{
		if (state.compression == Protocol::Compression::Enable)
			state.maybe_compressed_in = new CompressedReadBuffer(*in);
		else
			state.maybe_compressed_in = in;

		state.block_in = state.context.format_factory->getInput(
			"Native",
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


bool TCPHandler::isQueryCancelled()
{
	Poco::ScopedLock<Poco::FastMutex> lock(is_cancelled_mutex);

	if (state.is_cancelled || state.sent_all_data)
		return true;
	
	if (after_check_cancelled.elapsed() / 1000 < state.context.settings.interactive_delay)
		return false;

	after_check_cancelled.restart();
	
	/// Во время выполнения запроса, единственный пакет, который может прийти от клиента - это остановка выполнения запроса.
	if (in->poll(0))
	{
	//	std::cerr << "checking cancelled; socket has data" << std::endl;
		
		UInt64 packet_type = 0;
		readVarUInt(packet_type, *in);

		switch (packet_type)
		{
			case Protocol::Client::Cancel:
				if (state.empty())
					throw Exception("Unexpected packet Cancel received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
				LOG_INFO(log, "Query was cancelled.");
				state.is_cancelled = true;
				return true;

			default:
				throw Exception("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
		}
	}

	return false;
}


void TCPHandler::sendData(Block & block)
{
	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);
	
	if (!state.block_out)
	{
		if (state.compression == Protocol::Compression::Enable)
			state.maybe_compressed_out = new CompressedWriteBuffer(*out);
		else
			state.maybe_compressed_out = out;

		state.block_out = state.context.format_factory->getOutput(
			"Native",
			*state.maybe_compressed_out,
			state.io.in_sample);
	}

	writeVarUInt(Protocol::Server::Data, *out);

	state.block_out->write(block);
	state.maybe_compressed_out->next();
	out->next();
}


void TCPHandler::sendException(const Exception & e)
{
	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);
	
	writeVarUInt(Protocol::Server::Exception, *out);
	writeException(e, *out);
	out->next();
}


void TCPHandler::sendEndOfStream()
{
	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);

	state.sent_all_data = true;
	writeVarUInt(Protocol::Server::EndOfStream, *out);
	out->next();
}


void TCPHandler::sendProgress(size_t rows, size_t bytes)
{
	Poco::ScopedLock<Poco::FastMutex> lock(send_mutex);

	state.rows_processed += rows;
	state.bytes_processed += bytes;

	/// Не будем отправлять прогресс после того, как отправлены все данные.
	if (state.sent_all_data)
		return;

	if (after_send_progress.elapsed() / 1000 < state.context.settings.interactive_delay)
		return;

	after_send_progress.restart();
	
	writeVarUInt(Protocol::Server::Progress, *out);
	Progress progress(state.rows_processed, state.bytes_processed);
	progress.write(*out);
	out->next();

	state.rows_processed = 0;
	state.bytes_processed = 0;
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
