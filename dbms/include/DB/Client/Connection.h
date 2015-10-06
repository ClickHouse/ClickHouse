#pragma once

#include <common/logger_useful.h>

#include <Poco/Net/StreamSocket.h>

#include <DB/Common/Throttler.h>

#include <DB/Core/Block.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Progress.h>
#include <DB/Core/Protocol.h>
#include <DB/Core/QueryProcessingStage.h>

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataStreams/BlockStreamProfileInfo.h>

#include <DB/Interpreters/Settings.h>

#include <atomic>


namespace DB
{

using Poco::SharedPtr;

class ParallelReplicas;

/// Поток блоков читающих из таблицы и ее имя
typedef std::pair<BlockInputStreamPtr, std::string> ExternalTableData;
/// Вектор пар, описывающих таблицы
typedef std::vector<ExternalTableData> ExternalTablesData;


/** Соединение с сервером БД для использования в клиенте.
  * Как использовать - см. Core/Protocol.h
  * (Реализацию на стороне сервера - см. Server/TCPHandler.h)
  *
  * В качестве default_database может быть указана пустая строка
  *  - в этом случае сервер использует свою БД по-умолчанию.
  */
class Connection : private boost::noncopyable
{
	friend class ParallelReplicas;

public:
	Connection(const String & host_, UInt16 port_, const String & default_database_,
		const String & user_, const String & password_,
		const String & client_name_ = "client",
		Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
		Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
		Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
		Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0),
		Poco::Timespan ping_timeout_ = Poco::Timespan(DBMS_DEFAULT_PING_TIMEOUT_SEC, 0))
		:
		host(host_), port(port_), default_database(default_database_),
		user(user_), password(password_), resolved_address(host, port),
		client_name(client_name_),
		compression(compression_),
		connect_timeout(connect_timeout_), receive_timeout(receive_timeout_), send_timeout(send_timeout_),
		ping_timeout(ping_timeout_),
		log_wrapper(*this)
	{
		/// Соединеняемся не сразу, а при первой необходимости.

		if (user.empty())
			user = "default";

		setDescription();
	}

	Connection(const String & host_, UInt16 port_, const Poco::Net::SocketAddress & resolved_address_,
		const String & default_database_,
		const String & user_, const String & password_,
		const String & client_name_ = "client",
		Protocol::Compression::Enum compression_ = Protocol::Compression::Enable,
		Poco::Timespan connect_timeout_ = Poco::Timespan(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
		Poco::Timespan receive_timeout_ = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
		Poco::Timespan send_timeout_ = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0),
		Poco::Timespan ping_timeout_ = Poco::Timespan(DBMS_DEFAULT_PING_TIMEOUT_SEC, 0))
		:
		host(host_), port(port_),
		default_database(default_database_),
		user(user_), password(password_),
		resolved_address(resolved_address_),
		client_name(client_name_),
		compression(compression_),
		connect_timeout(connect_timeout_), receive_timeout(receive_timeout_), send_timeout(send_timeout_),
		ping_timeout(ping_timeout_),
		log_wrapper(*this)
	{
		/// Соединеняемся не сразу, а при первой необходимости.

		if (user.empty())
			user = "default";

		setDescription();
	}

	virtual ~Connection() {};

	/// Установить ограничитель сетевого трафика. Один ограничитель может использоваться одновременно для нескольких разных соединений.
	void setThrottler(const ThrottlerPtr & throttler_)
	{
		throttler = throttler_;
	}


	/// Пакет, который может быть получен от сервера.
	struct Packet
	{
		UInt64 type;

		Block block;
		SharedPtr<Exception> exception;
		Progress progress;
		BlockStreamProfileInfo profile_info;

		Packet() : type(Protocol::Server::Hello) {}
	};

	/// Изменить базу данных по умолчанию. Изменения начинают использоваться только при следующем переподключении.
	void setDefaultDatabase(const String & database);

	void getServerVersion(String & name, UInt64 & version_major, UInt64 & version_minor, UInt64 & revision);

	/// Для сообщений в логе и в эксепшенах.
	const String & getDescription() const
	{
		return description;
	}

	const String & getHost() const
	{
		return host;
	}

	UInt16 getPort() const
	{
		return port;
	}

	/// Если последний флаг true, то затем необходимо вызвать sendExternalTablesData
	void sendQuery(const String & query, const String & query_id_ = "", UInt64 stage = QueryProcessingStage::Complete,
		const Settings * settings = nullptr, bool with_pending_data = false);

	void sendCancel();
	/// Отправить блок данных, на сервере сохранить во временную таблицу name
	void sendData(const Block & block, const String & name = "");
	/// Отправить все содержимое внешних таблиц
	void sendExternalTablesData(ExternalTablesData & data);

	/// Отправить блок данных, который уже был заранее сериализован (и, если надо, сжат), который следует прочитать из input-а.
	/// можно передать размер сериализованного/сжатого блока.
	void sendPreparedData(ReadBuffer & input, size_t size,  const String & name = "");

	/// Проверить, есть ли данные, которые можно прочитать.
	bool poll(size_t timeout_microseconds = 0);

	/// Проверить, есть ли данные в буфере для чтения.
	bool hasReadBufferPendingData() const;

	/// Получить пакет от сервера.
	Packet receivePacket();

	/// Если ещё не соединено, или соединение разорвано - соединиться. Если не получилось - кинуть исключение.
	void forceConnected();

	/** Разорвать соединение.
	  * Это может быть нужно, например, чтобы соединение не осталось висеть в
	  *  рассинхронизированном состоянии (когда кто-то чего-то продолжает ждать) после эксепшена.
	  */
	void disconnect();

	size_t outBytesCount() const { return !out.isNull() ? out->count() : 0; }
	size_t inBytesCount() const { return !in.isNull() ? in->count() : 0; }

private:
	String host;
	UInt16 port;
	String default_database;
	String user;
	String password;

	/** Адрес может быть заранее отрезолвен и передан в конструктор. Тогда поля host и port имеют смысл только для логгирования.
	  * Иначе адрес резолвится в конструкторе. То есть, DNS балансировка не поддерживается.
	  */
	Poco::Net::SocketAddress resolved_address;

	/// Для сообщений в логе и в эксепшенах.
	String description;
	void setDescription();

	String client_name;

	bool connected = false;

	String server_name;
	UInt64 server_version_major = 0;
	UInt64 server_version_minor = 0;
	UInt64 server_revision = 0;

	Poco::Net::StreamSocket socket;
	SharedPtr<ReadBuffer> in;
	SharedPtr<WriteBuffer> out;

	String query_id;
	UInt64 compression;		/// Сжимать ли данные при взаимодействии с сервером.
	/// каким алгоритмом сжимать данные при INSERT и данные внешних таблиц
	CompressionMethod network_compression_method = CompressionMethod::LZ4;

	/** Если не nullptr, то используется, чтобы ограничить сетевой трафик.
	  * Учитывается только трафик при передаче блоков. Другие пакеты не учитываются.
	  */
	ThrottlerPtr throttler;

	Poco::Timespan connect_timeout;
	Poco::Timespan receive_timeout;
	Poco::Timespan send_timeout;
	Poco::Timespan ping_timeout;

	/// Откуда читать результат выполнения запроса.
	SharedPtr<ReadBuffer> maybe_compressed_in;
	BlockInputStreamPtr block_in;

	/// Куда писать данные INSERT-а.
	SharedPtr<WriteBuffer> maybe_compressed_out;
	BlockOutputStreamPtr block_out;

	/// логгер, создаваемый лениво, чтобы не обращаться к DNS в конструкторе
	class LoggerWrapper
	{
	public:
		LoggerWrapper(Connection & parent_)
			: log(nullptr), parent(parent_)
		{
		}

		Logger * get()
		{
			if (!log)
				log = &Logger::get("Connection (" + parent.getDescription() + ")");

			return log;
		}

	private:
		std::atomic<Logger *> log;
		Connection & parent;
	};

	LoggerWrapper log_wrapper;

	void connect();
	void sendHello();
	void receiveHello();
	bool ping();

	Block receiveData();
	SharedPtr<Exception> receiveException();
	Progress receiveProgress();
	BlockStreamProfileInfo receiveProfileInfo();

	void initBlockInput();
};


typedef SharedPtr<Connection> ConnectionPtr;
typedef std::vector<ConnectionPtr> Connections;

}
