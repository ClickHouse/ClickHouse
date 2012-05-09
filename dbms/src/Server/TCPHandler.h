#pragma once

#include <DB/Core/Protocol.h>

#include <DB/IO/ChunkedReadBuffer.h>
#include <DB/IO/ChunkedWriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBufferFromPocoSocket.h>

#include <DB/DataStreams/BlockIO.h>

#include "Server.h"


namespace DB
{


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

	/** Исключение во время выполнения запроса (его надо отдать по сети клиенту).
	  * Клиент сможет его принять, если оно не произошло во время отправки другого пакета.
	  */
	SharedPtr<Exception> exception;
	

	QueryState() : query_id(0), stage(Protocol::QueryProcessingStage::Complete), compression(Protocol::Compression::Disable) {}
	
	void reset()
	{
		*this = QueryState();
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

	Poco::FastMutex is_cancelled_mutex;
	

	void runImpl();

	void sendHello(WriteBuffer & out);
	bool sendData(WriteBuffer & out, WriteBuffer & out_for_chunks);
	void sendException(WriteBuffer & out);
	void sendProgress(WriteBuffer & out);
	void sendOk(WriteBuffer & out);

	bool receivePacket(ReadBuffer & in);
	void receiveQuery(ReadBuffer & in);
	bool receiveData(ReadBuffer & in);

	bool isQueryCancelled(ReadBufferFromPocoSocket & in);
};


}
