#pragma once

#include <DB/Core/Protocol.h>
#include <DB/Core/QueryProcessingStage.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/BlockIO.h>

#include <DB/Common/Stopwatch.h>
#include <DB/Common/CurrentMetrics.h>

#include "Server.h"


namespace CurrentMetrics
{
	extern const Metric TCPConnection;
}


namespace DB
{


/// State of query processing.
struct QueryState
{
	/// Identifier of the query.
	String query_id;

	QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;
	Protocol::Compression::Enum compression = Protocol::Compression::Disable;

	/// From where to read data for INSERT.
	std::shared_ptr<ReadBuffer> maybe_compressed_in;
	BlockInputStreamPtr block_in;

	/// Where to write result data.
	std::shared_ptr<WriteBuffer> maybe_compressed_out;
	BlockOutputStreamPtr block_out;

	/// Query text.
	String query;
	/// Streams of blocks, that are processing the query.
	BlockIO io;

	/// Отменен ли запрос
	bool is_cancelled = false;
	/// Пустой или нет
	bool is_empty = true;
	/// Данные были отправлены.
	bool sent_all_data = false;
	/// Запрос требует приёма данных от клиента (INSERT, но не INSERT SELECT).
	bool need_receive_data_for_insert = false;

	/// Для вывода прогресса - разница после предыдущей отправки прогресса.
	Progress progress;


	void reset()
	{
		*this = QueryState();
	}

	bool empty()
	{
		return is_empty;
	}
};


class TCPHandler : public Poco::Net::TCPServerConnection
{
public:
	TCPHandler(Server & server_, const Poco::Net::StreamSocket & socket_)
		: Poco::Net::TCPServerConnection(socket_), server(server_),
		log(&Logger::get("TCPHandler")), client_revision(0),
		connection_context(*server.global_context), query_context(connection_context)
	{
	}

	void run();

private:
	Server & server;
	Logger * log;

	String client_name;
	UInt64 client_version_major = 0;
	UInt64 client_version_minor = 0;
	UInt64 client_revision = 0;

	Context connection_context;
	Context query_context;

	/// Потоки для чтения/записи из/в сокет соединения с клиентом.
	std::shared_ptr<ReadBuffer> in;
	std::shared_ptr<WriteBuffer> out;

	/// Время после последней проверки остановки запроса и отправки прогресса.
	Stopwatch after_check_cancelled;
	Stopwatch after_send_progress;

	String default_database;

	/// На данный момент, поддерживается одновременное выполнение только одного запроса в соединении.
	QueryState state;

	CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};


	void runImpl();

	void receiveHello();
	bool receivePacket();
	void receiveQuery();
	bool receiveData();
	void readData(const Settings & global_settings);

	/// Обработать запрос INSERT
	void processInsertQuery(const Settings & global_settings);

	/// Обработать запрос, который не требует приёма блоков данных от клиента
	void processOrdinaryQuery();

	void sendHello();
	void sendData(Block & block);	/// Записать в сеть блок.
	void sendException(const Exception & e);
	void sendProgress();
	void sendEndOfStream();
	void sendProfileInfo();
	void sendTotals();
	void sendExtremes();

	/// Создаёт state.block_in/block_out для чтения/записи блоков, в зависимости от того, включено ли сжатие.
	void initBlockInput();
	void initBlockOutput();

	bool isQueryCancelled();

	/// Эта функция вызывается из разных потоков.
	void updateProgress(const Progress & value);
};


}
