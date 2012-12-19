#pragma once

#include <DB/Core/Protocol.h>
#include <DB/Core/QueryProcessingStage.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromPocoSocket.h>

#include <DB/DataStreams/BlockIO.h>

#include <statdaemons/Stopwatch.h>

#include "Server.h"


namespace DB
{


/// Состояние обработки запроса.
struct QueryState
{
	/// Идентификатор запроса.
	UInt64 query_id;

	QueryProcessingStage::Enum stage;
	Protocol::Compression::Enum compression;

	/// Откуда читать данные для INSERT-а.
	SharedPtr<ReadBuffer> maybe_compressed_in;
	BlockInputStreamPtr block_in;

	/// Куда писать возвращаемые данные.
	SharedPtr<WriteBuffer> maybe_compressed_out;
	BlockOutputStreamPtr block_out;

	/// Текст запроса.
	String query;
	/// Потоки блоков, с помощью которых выполнять запрос.
	BlockIO io;

	bool is_cancelled;
	/// Данные были отправлены.
	bool sent_all_data;

	/// Для вывода прогресса - разница после предыдущей отправки прогресса.
	size_t rows_processed;
	size_t bytes_processed;
	

	QueryState() : query_id(0), stage(QueryProcessingStage::Complete), compression(Protocol::Compression::Disable),
		is_cancelled(false), sent_all_data(false), rows_processed(0), bytes_processed(0) {}
	
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
		, log(&Logger::get("TCPHandler")), connection_context(server.global_context)
	{
	    LOG_TRACE(log, "In constructor.");
	}

	void run();

private:
	Server & server;
	Logger * log;

	Context connection_context;

	SharedPtr<ReadBufferFromPocoSocket> in;
	SharedPtr<WriteBufferFromPocoSocket> out;

	/// Для сериализации пакетов "данные" и "прогресс" (пакет типа "прогресс" может отправляться из другого потока).
	Poco::FastMutex send_mutex;

	/// Время после последней проверки остановки запроса и отправки прогресса.
	Stopwatch after_check_cancelled;
	Stopwatch after_send_progress;

	String default_database;

	/// На данный момент, поддерживается одновременное выполнение только одного запроса в соединении.
	QueryState state;


	void runImpl();

	void receiveHello();
	bool receivePacket();
	void receiveQuery();
	bool receiveData();

	/// Обработать запрос INSERT
	void processInsertQuery();

	/// Обработать запрос, который не требует приёма блоков данных от клиента
	void processOrdinaryQuery();

	void sendHello();
	void sendData(Block & block);	/// Записать в сеть блок.
	void sendException(const Exception & e);
	void sendProgress(size_t rows, size_t bytes);
	void sendEndOfStream();

	bool isQueryCancelled();

	/// Вывести информацию о скорости выполнения SELECT запроса.
	void logProfileInfo(Stopwatch & watch, IBlockInputStream & in);
};


}
