#pragma once

#include <DB/Core/Protocol.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/BlockIO.h>

#include "Server.h"


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
  * Нельзя использовать out напрямую.
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

		std::cerr << "?" << std::endl;

		writeIntBinary(query_id, out);
		writeIntBinary(false, out);
		writeIntBinary(offset(), out);

		out.position() = position();
		out.next();
		working_buffer = Buffer(out.buffer().begin() + headerSize(), out.buffer().end());
		pos = working_buffer.begin();
	}

public:
	ChunkedWriteBuffer(WriteBuffer & out_, UInt64 query_id_)
		: WriteBuffer(out_.position() + headerSize(), out_.buffer().size() - out_.offset() - headerSize()), out(out_), query_id(query_id_)
	{
		checkBufferSize();
	}

	void finish()
	{
		next();
		
		writeIntBinary(query_id, out);
		writeIntBinary(true, out);
		writeIntBinary(static_cast<size_t>(0), out);
	}
};


/// Состояние обработки запроса.
struct QueryState
{
	/// Идентификатор запроса.
	UInt64 query_id;

	Protocol::QueryProcessingStage::Enum stage;
	Protocol::Compression::Enum compression;
	String in_format;
	String out_format;

	/// Откуда читать данные для INSERT-а.
	SharedPtr<ReadBuffer> chunked_in;
	SharedPtr<ReadBuffer> maybe_compressed_in;
	BlockInputStreamPtr block_in;

	/// Куда писать возвращаемые данные.
	SharedPtr<WriteBuffer> chunked_out;
	SharedPtr<WriteBuffer> maybe_compressed_out;
	BlockOutputStreamPtr block_out;

	/// Текст запроса.
	String query;
	/// Потоки блоков, с помощью которых выполнять запрос.
	BlockIO io;

	Context context;

	QueryState() : query_id(0), stage(Protocol::QueryProcessingStage::Complete), compression(Protocol::Compression::Disable) {}
	
	void reset()
	{
		query_id = 0;
		stage = Protocol::QueryProcessingStage::Complete;
		compression = Protocol::Compression::Disable;
		in_format.clear();
		out_format.clear();
		query.clear();
		io = BlockIO();
	}

	bool empty()
	{
		return query_id == 0;
	}
};


class TCPHandler : public Poco::Net::TCPServerConnection
{
public:
	TCPHandler(Server & server_, const Poco::Net::StreamSocket & socket_)
		: Poco::Net::TCPServerConnection(socket_), server(server_)
		, log(&Logger::get("TCPHandler"))
	{
	    LOG_TRACE(log, "In constructor.");
	}

	void run();

private:
	Server & server;
	Logger * log;

	/// На данный момент, поддерживается одновременное выполнение только одного запроса в соединении.
	QueryState state;

	void runImpl();

	void sendHello(WriteBuffer & out);
	bool sendData(WriteBuffer & out, WriteBuffer & out_for_chunks);
	void sendException(WriteBuffer & out);
	void sendProgress(WriteBuffer & out);

	bool receivePacket(ReadBuffer & in);
	void receiveQuery(ReadBuffer & in);
	bool receiveData(ReadBuffer & in);
};


}
