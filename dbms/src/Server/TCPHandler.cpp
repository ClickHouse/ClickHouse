#include <Yandex/Revision.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromPocoSocket.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>

#include <DB/Interpreters/executeQuery.h>

#include "TCPHandler.h"


namespace DB
{


/** Считывает данные, из формата, состоящего из чанков
  * (идентификатор запроса, признак последнего чанка, размер чанка, часть данных со сжатием или без).
  */
class ChunkedReadBuffer : public ReadBuffer
{
protected:
	ReadBuffer & in;
	bool all_read;
	size_t read_in_chunk;
	size_t chunk_size;
	UInt64 assert_query_id;

	bool nextImpl()
	{
		/// Если прочитали ещё не весь блок - получим следующие данные. Если следующих данных нет - ошибка.
		if (read_in_chunk < chunk_size)
		{
			if (!in.next())
				throw Exception("Cannot read all query", ErrorCodes::CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT);

			working_buffer = in.buffer();
			if (chunk_size - read_in_chunk < working_buffer.size())
			{
				working_buffer.resize(chunk_size - read_in_chunk);
				read_in_chunk = chunk_size;
			}
			else
				read_in_chunk += working_buffer.size();
		}
		else
		{
			if (all_read)
				return false;

			UInt64 query_id = 0;
			readIntBinary(query_id, in);
			if (query_id != assert_query_id)
				throw Exception("Received data for wrong query id", ErrorCodes::RECEIVED_DATA_FOR_WRONG_QUERY_ID);

			/// Флаг конца.
			readIntBinary(all_read, in);
			/// Размер блока.
			readIntBinary(chunk_size, in);

			read_in_chunk = std::min(chunk_size, in.buffer().size() - in.offset());
			working_buffer = Buffer(in.position(), in.position() + read_in_chunk);
			in.position() += read_in_chunk;
		}
		
		return true;
	}

public:
	ChunkedReadBuffer(ReadBuffer & in_, UInt64 assert_query_id_)
		: ReadBuffer(NULL, 0), in(in_), all_read(false), read_in_chunk(0), chunk_size(0), assert_query_id(assert_query_id_) {}
};


/** Записывает данные в формат, состоящий из чанков
  * (идентификатор запроса, признак последнего чанка, размер чанка, часть данных со сжатием или без).
  */
class ChunkedWriteBuffer : public WriteBuffer
{
protected:
	WriteBuffer & out;
	UInt64 query_id;

	inline size_t headerSize() { return sizeof(query_id) + sizeof(bool) + sizeof(size_t); }

	void checkBufferSize()
	{
		if (out.buffer().end() - out.position() < 2 * static_cast<int>(headerSize()))
			throw Exception("Too small remaining buffer size to write chunked data", ErrorCodes::TOO_SMALL_BUFFER_SIZE);
	}

	void nextImpl()
	{
		checkBufferSize();

		writeIntBinary(query_id, out);
		writeIntBinary(false, out);
		writeIntBinary(offset(), out);
		
		out.position() = position();
		out.next();
		working_buffer = Buffer(out.buffer().begin() + headerSize(), out.buffer().end());
	}

public:
	ChunkedWriteBuffer(WriteBuffer & out_, UInt64 query_id_)
		: WriteBuffer(out.buffer().begin() + headerSize(), out.buffer().size() - headerSize()), out(out_), query_id(query_id_)
	{
		checkBufferSize();
	}

	void writeEnd()
	{
		writeIntBinary(query_id, out);
		writeIntBinary(true, out);
		writeIntBinary(static_cast<size_t>(0), out);
	}
};


void TCPHandler::runImpl()
{
	ReadBufferFromPocoSocket in(socket());
	WriteBufferFromPocoSocket out(socket());
	
	/// Сразу после соединения, отправляем hello-пакет.
	sendHello(out);

	/*while (1)
	{
		/// Считываем заголовок запроса: идентификатор запроса и информацию, до какой стадии выполнять запрос.
		UInt64 query_id = 0;
		UInt64 query_processing_stage = Protocol::QueryProcessingStage::Complete;
		readIntBinary(query_id, in);
		readVarUInt(query_processing_stage, in);

		LOG_DEBUG(log, "Query ID: " << query_id);

		QueryReader query_reader(in);

		BlockInputStreamPtr query_plan;
		Context context = server.global_context;

		Stopwatch watch;
		executeQuery(query_reader, out, context, query_plan);
		watch.stop();
	}*/
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

void TCPHandler::receivePacket(ReadBuffer & in)
{
	UInt64 packet_type = 0;
	readVarUInt(packet_type, in);

	switch (packet_type)
	{
		case Protocol::Client::Query:
			if (!state.empty())
				throw Exception("Unexpected packet Query received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
			receiveQuery(in);
			break;
			
		case Protocol::Client::Data:
			if (state.empty())
				throw Exception("Unexpected packet Data received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
			receiveData(in);
			break;
			
		default:
			throw Exception("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
	}
}

void TCPHandler::receiveQuery(ReadBuffer & in)
{
	UInt64 stage = 0;
	UInt64 compression = 0;
	
	readIntBinary(state.query_id, in);

	readVarUInt(stage, in);
	state.stage = Protocol::QueryProcessingStage::Enum(stage);

	readVarUInt(compression, in);
	state.compression = Protocol::Compression::Enum(compression);

	readStringBinary(state.in_format, in);
	readStringBinary(state.out_format, in);
	
	readStringBinary(state.query, in);
}

void TCPHandler::receiveData(ReadBuffer & in)
{
	SharedPtr<ReadBuffer> chunked_in = new ChunkedReadBuffer(in, state.query_id);
	SharedPtr<ReadBuffer> maybe_compressed_in = state.compression == Protocol::Compression::Enable
		? new CompressedReadBuffer(*chunked_in)
		: chunked_in;

	//BlockInputStreamPtr block_in = state.context.format_factory->getInput();

	// TODO
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
