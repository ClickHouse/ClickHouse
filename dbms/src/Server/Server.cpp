#include <sys/resource.h>
#include <sys/file.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/DNS.h>
#include <Poco/Util/XMLConfiguration.h>

#include <common/ApplicationServerExt.h>
#include <common/ErrorHandlers.h>
#include <common/Revision.h>

#include <DB/Common/ConfigProcessor.h>
#include <ext/scope_guard.hpp>

#include <memory>
#include <thread>
#include <atomic>
#include <condition_variable>

#include <DB/Common/Macros.h>
#include <DB/Common/getFQDNOrHostName.h>
#include <DB/Common/setThreadName.h>
#include <DB/Interpreters/loadMetadata.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Storages/System/StorageSystemNumbers.h>
#include <DB/Storages/System/StorageSystemTables.h>
#include <DB/Storages/System/StorageSystemParts.h>
#include <DB/Storages/System/StorageSystemDatabases.h>
#include <DB/Storages/System/StorageSystemProcesses.h>
#include <DB/Storages/System/StorageSystemEvents.h>
#include <DB/Storages/System/StorageSystemOne.h>
#include <DB/Storages/System/StorageSystemMerges.h>
#include <DB/Storages/System/StorageSystemSettings.h>
#include <DB/Storages/System/StorageSystemZooKeeper.h>
#include <DB/Storages/System/StorageSystemReplicas.h>
#include <DB/Storages/System/StorageSystemReplicationQueue.h>
#include <DB/Storages/System/StorageSystemDictionaries.h>
#include <DB/Storages/System/StorageSystemColumns.h>
#include <DB/Storages/System/StorageSystemFunctions.h>
#include <DB/Storages/System/StorageSystemClusters.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>

#include <DB/IO/copyData.h>
#include <DB/IO/LimitReadBuffer.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/Operators.h>

#include <zkutil/ZooKeeper.h>

#include "Server.h"
#include "HTTPHandler.h"
#include "InterserverIOHTTPHandler.h"
#include "OLAPHTTPHandler.h"
#include "TCPHandler.h"


namespace
{

/**	Automatically sends difference of ProfileEvents to Graphite at beginning of every minute
*/
class ProfileEventsTransmitter
{
public:
	~ProfileEventsTransmitter()
	{
		try
		{
			{
				std::lock_guard<std::mutex> lock{mutex};
				quit = true;
			}

			cond.notify_one();

			thread.join();
		}
		catch (...)
		{
			DB::tryLogCurrentException(__FUNCTION__);
		}
	}

private:
	void run()
	{
		setThreadName("ProfileEventsTx");

		const auto get_next_minute = [] {
			return std::chrono::time_point_cast<std::chrono::minutes, std::chrono::system_clock>(
				std::chrono::system_clock::now() + std::chrono::minutes(1)
			);
		};

		std::unique_lock<std::mutex> lock{mutex};

		while (true)
		{
			if (cond.wait_until(lock, get_next_minute(), [this] { return quit; }))
				break;

			transmitCounters();
		}
	}

	void transmitCounters()
	{
		GraphiteWriter::KeyValueVector<size_t> key_vals{};
		key_vals.reserve(ProfileEvents::END);

		for (size_t i = 0; i < ProfileEvents::END; ++i)
		{
			const auto counter = ProfileEvents::counters[i];
			const auto counter_increment = counter - prev_counters[i];
			prev_counters[i] = counter;

			std::string key{ProfileEvents::getDescription(static_cast<ProfileEvents::Event>(i))};
			key_vals.emplace_back(event_path_prefix + key, counter_increment);
		}

		Daemon::instance().writeToGraphite(key_vals);
	}

	/// Значения счётчиков при предыдущей отправке (или нули, если ни разу не отправляли).
	decltype(ProfileEvents::counters) prev_counters{};

	bool quit = false;
	std::mutex mutex;
	std::condition_variable cond;
	std::thread thread{&ProfileEventsTransmitter::run, this};

	static constexpr auto event_path_prefix = "ClickHouse.ProfileEvents.";
};

}

namespace DB
{

/** Каждые две секунды проверяет, не изменился ли конфиг.
  *  Когда изменился, запускает на нем ConfigProcessor и вызывает setUsersConfig у контекста.
  * NOTE: Не перезагружает конфиг, если изменились другие файлы, влияющие на обработку конфига: metrika.xml
  *  и содержимое conf.d и users.d. Это можно исправить, переместив проверку времени изменения файлов в ConfigProcessor.
  */
class UsersConfigReloader
{
public:
	UsersConfigReloader(const std::string & path, Context * context);
	~UsersConfigReloader();
private:
	std::string path;
	Context * context;

	time_t file_modification_time;
	std::atomic<bool> quit;
	std::thread thread;

	Logger * log;

	void reloadIfNewer(bool force);
	void run();
};


/// Отвечает "Ok.\n". Используется для проверки живости.
class PingRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
	{
		try
		{
			const char * data = "Ok.\n";
			response.sendBuffer(data, strlen(data));
		}
		catch (...)
		{
			tryLogCurrentException("PingRequestHandler");
		}
	}
};


/// Отвечает "Ok.\n", если все реплики на этом сервере не слишком сильно отстают. Иначе выводит информацию об отставании. TODO Вынести в отдельный файл.
class ReplicasStatusHandler : public Poco::Net::HTTPRequestHandler
{
private:
	Context & context;

public:
	ReplicasStatusHandler(Context & context_)
		: context(context_)
	{
	}

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
	{
		try
		{
			/// Собираем набор реплицируемых таблиц.
			Databases replicated_tables;
			{
				Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());

				for (const auto & db : context.getDatabases())
					for (const auto & table : db.second)
						if (typeid_cast<const StorageReplicatedMergeTree *>(table.second.get()))
							replicated_tables[db.first][table.first] = table.second;
			}

			const MergeTreeSettings & settings = context.getMergeTreeSettings();

			bool ok = /*true*/false;
			std::stringstream message;

			for (const auto & db : replicated_tables)
			{
				for (const auto & table : db.second)
				{
					time_t absolute_delay = 0;
					time_t relative_delay = 0;

					static_cast<const StorageReplicatedMergeTree &>(*table.second).getReplicaDelays(absolute_delay, relative_delay);

					if ((settings.min_absolute_delay_to_close && absolute_delay >= static_cast<time_t>(settings.min_absolute_delay_to_close))
						|| (settings.min_relative_delay_to_close && relative_delay >= static_cast<time_t>(settings.min_relative_delay_to_close)))
						ok = false;

					message << backQuoteIfNeed(db.first) << "." << backQuoteIfNeed(table.first)
						<< "\tAbsolute delay: " << absolute_delay << ". Relative delay: " << relative_delay << ".\n";
				}
			}

			if (ok)
			{
				const char * data = "Ok.\n";
				response.sendBuffer(data, strlen(data));
			}
			else
			{
				response.send() << message.rdbuf();
			}
		}
		catch (...)
		{
			/// TODO Отправлять клиенту.
			tryLogCurrentException("ReplicasStatusHandler");
		}
	}
};


/// Отвечает 404 с подробным объяснением.
class NotFoundHandler : public Poco::Net::HTTPRequestHandler
{
public:
	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
	{
		try
		{
			response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
			response.send() << "There is no handle " << request.getURI() << "\n\n"
				<< "Use / or /ping for health checks.\n"
				<< "Or /replicas_status for more sophisticated health checks.\n\n"
				<< "Send queries from your program with POST method or GET /?query=...\n\n"
				<< "Use clickhouse-client:\n\n"
				<< "For interactive data analysis:\n"
				<< "    clickhouse-client\n\n"
				<< "For batch query processing:\n"
				<< "    clickhouse-client --query='SELECT 1' > result\n"
				<< "    clickhouse-client < query > result\n";
		}
		catch (...)
		{
			tryLogCurrentException("NotFoundHandler");
		}
	}
};


template <typename HandlerType>
class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
	Server & server;
	Logger * log;
	std::string name;

public:
	HTTPRequestHandlerFactory(Server & server_, const std::string & name_)
		: server(server_), log(&Logger::get(name_)), name(name_) {}

	Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request)
	{
		LOG_TRACE(log, "HTTP Request for " << name << ". "
			<< "Method: " << request.getMethod()
			<< ", Address: " << request.clientAddress().toString()
			<< ", User-Agent: " << (request.has("User-Agent") ? request.get("User-Agent") : "none"));

		const auto & uri = request.getURI();

		if (uri.find('?') != std::string::npos
			|| request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
		{
			return new HandlerType(server);
		}
		else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
			|| request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
		{
			if (uri == "/" || uri == "/ping")
				return new PingRequestHandler;
			else if (uri == "/replicas_status")
				return new ReplicasStatusHandler(*server.global_context);
			else
				return new NotFoundHandler;
		}
		else
			return nullptr;
	}
};


class TCPConnectionFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
	Server & server;
	Logger * log;

public:
	TCPConnectionFactory(Server & server_) : server(server_), log(&Logger::get("TCPConnectionFactory")) {}

	Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket)
	{
		LOG_TRACE(log, "TCP Request. " << "Address: " << socket.peerAddress().toString());

		return new TCPHandler(server, socket);
	}
};


UsersConfigReloader::UsersConfigReloader(const std::string & path_, Context * context_)
	: path(path_), context(context_), file_modification_time(0), quit(false), log(&Logger::get("UsersConfigReloader"))
{
	/// Если путь к конфигу не абсолютный, угадаем, относительно чего он задан.
	/// Сначала поищем его рядом с основным конфигом, потом - в текущей директории.
	if (path.empty() || path[0] != '/')
	{
		std::string main_config_path = Application::instance().config().getString("config-file", "config.xml");
		std::string config_dir = Poco::Path(main_config_path).parent().toString();
		if (Poco::File(config_dir + path).exists())
			path = config_dir + path;
	}

	reloadIfNewer(true);
	thread = std::thread(&UsersConfigReloader::run, this);
}

UsersConfigReloader::~UsersConfigReloader()
{
	try
	{
		quit = true;
		thread.join();
	}
	catch (...)
	{
		tryLogCurrentException("~UsersConfigReloader");
	}
}

void UsersConfigReloader::run()
{
	setThreadName("UserConfReload");

	while (!quit)
	{
		std::this_thread::sleep_for(std::chrono::seconds(2));
		reloadIfNewer(false);
	}
}

void UsersConfigReloader::reloadIfNewer(bool force)
{
	Poco::File f(path);
	if (!f.exists())
	{
		if (force)
			throw Exception("Users config not found at: " + path, ErrorCodes::FILE_DOESNT_EXIST);
		if (file_modification_time)
		{
			LOG_ERROR(log, "Users config not found at: " << path);
			file_modification_time = 0;
		}
		return;
	}
	time_t new_modification_time = f.getLastModified().epochTime();
	if (!force && new_modification_time == file_modification_time)
		return;
	file_modification_time = new_modification_time;

	LOG_DEBUG(log, "Loading users config");

	ConfigurationPtr config;

	try
	{
		config = ConfigProcessor(!force).loadConfig(path);
	}
	catch (Poco::Exception & e)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error loading users config: " << e.what() << ": " << e.displayText());
		return;
	}
	catch (...)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error loading users config.");
		return;
	}

	try
	{
		context->setUsersConfig(config);
	}
	catch (Exception & e)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error updating users config: " << e.what() << ": " << e.displayText() << "\n" << e.getStackTrace().toString());
	}
	catch (Poco::Exception & e)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error updating users config: " << e.what() << ": " << e.displayText());
	}
	catch (...)
	{
		if (force)
			throw;

		LOG_ERROR(log, "Error updating users config.");
	}
}


/** Обеспечивает, что с одной директорией с данными может одновременно работать не более одного сервера.
  */
class StatusFile : private boost::noncopyable
{
public:
	StatusFile(const std::string & path_)
		: path(path_)
	{
		/// Если файл уже существует. NOTE Незначительный race condition.
		if (Poco::File(path).exists())
		{
			std::string contents;
			{
				ReadBufferFromFile in(path, 1024);
				LimitReadBuffer limit_in(in, 1024);
				WriteBufferFromString out(contents);
				copyData(limit_in, out);
			}

			if (!contents.empty())
				LOG_INFO(&Logger::get("StatusFile"), "Status file " << path << " already exists - unclean restart. Contents:\n" << contents);
			else
				LOG_INFO(&Logger::get("StatusFile"), "Status file " << path << " already exists and is empty - probably unclean hardware restart.");
		}

		fd = open(path.c_str(), O_WRONLY | O_CREAT, 0666);

		if (-1 == fd)
			throwFromErrno("Cannot open file " + path);

		try
		{
			int flock_ret = flock(fd, LOCK_EX | LOCK_NB);
			if (-1 == flock_ret)
			{
				if (errno == EWOULDBLOCK)
					throw Exception("Cannot lock file " + path + ". Another server instance in same directory is already running.");
				else
					throwFromErrno("Cannot lock file " + path);
			}

			if (0 != ftruncate(fd, 0))
				throwFromErrno("Cannot ftruncate " + path);

			if (0 != lseek(fd, 0, SEEK_SET))
				throwFromErrno("Cannot lseek " + path);

			/// Записываем в файл информацию о текущем экземпляре сервера.
			{
				WriteBufferFromFileDescriptor out(fd, 1024);
				out
					<< "PID: " << getpid() << "\n"
					<< "Started at: " << mysqlxx::DateTime(time(0)) << "\n"
					<< "Revision: " << Revision::get() << "\n";
			}
		}
		catch (...)
		{
			close(fd);
			throw;
		}
	}

	~StatusFile()
	{
		char buf[128];

		if (0 != close(fd))
			LOG_ERROR(&Logger::get("StatusFile"), "Cannot close file " << path << ", errno: "
				<< errno << ", strerror: " << strerror_r(errno, buf, sizeof(buf)));

		if (0 != unlink(path.c_str()))
			LOG_ERROR(&Logger::get("StatusFile"), "Cannot unlink file " << path << ", errno: "
				<< errno << ", strerror: " << strerror_r(errno, buf, sizeof(buf)));
	}

private:
	const std::string path;
	int fd = -1;
};


int Server::main(const std::vector<std::string> & args)
{
	Logger * log = &logger();

	std::string path = config().getString("path");
	Poco::trimInPlace(path);
	if (path.empty())
		throw Exception("path configuration parameter is empty");
	if (path.back() != '/')
		path += '/';

	StatusFile status{path + "status"};

	/// Попробуем повысить ограничение на число открытых файлов.
	{
		rlimit rlim;
		if (getrlimit(RLIMIT_NOFILE, &rlim))
			throw Poco::Exception("Cannot getrlimit");

		if (rlim.rlim_cur == rlim.rlim_max)
		{
			LOG_DEBUG(log, "rlimit on number of file descriptors is " << rlim.rlim_cur);
		}
		else
		{
			rlim_t old = rlim.rlim_cur;
			rlim.rlim_cur = rlim.rlim_max;
			if (setrlimit(RLIMIT_NOFILE, &rlim))
				throw Poco::Exception("Cannot setrlimit");

			LOG_DEBUG(log, "Set rlimit on number of file descriptors to " << rlim.rlim_cur << " (was " << old << ")");
		}
	}

	static ServerErrorHandler error_handler;
	Poco::ErrorHandler::set(&error_handler);

	/// Заранее инициализируем DateLUT, чтобы первая инициализация потом не влияла на измеряемую скорость выполнения.
	LOG_DEBUG(log, "Initializing DateLUT.");
	DateLUT::instance();
	LOG_TRACE(log, "Initialized DateLUT.");

	global_context.reset(new Context);

	/** Контекст содержит всё, что влияет на обработку запроса:
	  *  настройки, набор функций, типов данных, агрегатных функций, баз данных...
	  */
	global_context->setGlobalContext(*global_context);
	global_context->setPath(path);

	/// Директория для временных файлов при обработке тяжёлых запросов.
	{
		std::string tmp_path = config().getString("tmp_path", path + "tmp/");
		global_context->setTemporaryPath(tmp_path);
		Poco::File(tmp_path).createDirectories();

		/// Очистка временных файлов.
		Poco::DirectoryIterator dir_end;
		for (Poco::DirectoryIterator it(tmp_path); it != dir_end; ++it)
		{
			if (it->isFile() && 0 == it.name().compare(0, 3, "tmp"))
			{
				LOG_DEBUG(log, "Removing old temporary file " << it->path());
				it->remove();
			}
		}
	}

	bool has_zookeeper = false;
	if (config().has("zookeeper"))
	{
		global_context->setZooKeeper(std::make_shared<zkutil::ZooKeeper>(config(), "zookeeper"));
		has_zookeeper = true;
	}

	if (config().has("interserver_http_port"))
	{
		String this_host = config().getString("interserver_http_host", "");

		if (this_host.empty())
		{
			this_host = getFQDNOrHostName();
			LOG_DEBUG(log, "Configuration parameter 'interserver_http_host' doesn't exist or exists and empty. Will use '" + this_host + "' as replica host.");
		}

		String port_str = config().getString("interserver_http_port");
		int port = parse<int>(port_str);

		if (port < 0 || port > 0xFFFF)
			throw Exception("Out of range 'interserver_http_port': " + toString(port), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		global_context->setInterserverIOAddress(this_host, port);
	}

	if (config().has("macros"))
		global_context->setMacros(Macros(config(), "macros"));

	std::string users_config_path = config().getString("users_config", config().getString("config-file", "config.xml"));
	auto users_config_reloader = std::make_unique<UsersConfigReloader>(users_config_path, global_context.get());

	/// Максимальное количество одновременно выполняющихся запросов.
	global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

	/// Размер кэша разжатых блоков. Если нулевой - кэш отключён.
	size_t uncompressed_cache_size = parse<size_t>(config().getString("uncompressed_cache_size", "0"));
	if (uncompressed_cache_size)
		global_context->setUncompressedCache(uncompressed_cache_size);

	/// Размер кэша засечек. Обязательный параметр.
	size_t mark_cache_size = parse<size_t>(config().getString("mark_cache_size"));
	if (mark_cache_size)
		global_context->setMarkCache(mark_cache_size);

	/// Загружаем настройки.
	Settings & settings = global_context->getSettingsRef();
	global_context->setSetting("profile", config().getString("default_profile", "default"));

	LOG_INFO(log, "Loading metadata.");
	loadMetadata(*global_context);
	LOG_DEBUG(log, "Loaded metadata.");

	/// Создаём системные таблицы.
	if (!global_context->isDatabaseExist("system"))
	{
		Poco::File(path + "data/system").createDirectories();
		Poco::File(path + "metadata/system").createDirectories();
		global_context->addDatabase("system");
	}

	global_context->addTable("system", "one",		StorageSystemOne::create("one"));
	global_context->addTable("system", "numbers", 	StorageSystemNumbers::create("numbers"));
	global_context->addTable("system", "numbers_mt", StorageSystemNumbers::create("numbers_mt", true));
	global_context->addTable("system", "tables", 	StorageSystemTables::create("tables"));
	global_context->addTable("system", "parts", 	StorageSystemParts::create("parts"));
	global_context->addTable("system", "databases", StorageSystemDatabases::create("databases"));
	global_context->addTable("system", "processes", StorageSystemProcesses::create("processes"));
	global_context->addTable("system", "settings", 	StorageSystemSettings::create("settings"));
	global_context->addTable("system", "events", 	StorageSystemEvents::create("events"));
	global_context->addTable("system", "merges",	StorageSystemMerges::create("merges"));
	global_context->addTable("system", "replicas",	StorageSystemReplicas::create("replicas"));
	global_context->addTable("system", "replication_queue", StorageSystemReplicationQueue::create("replication_queue"));
	global_context->addTable("system", "dictionaries", StorageSystemDictionaries::create("dictionaries"));
	global_context->addTable("system", "columns",   StorageSystemColumns::create("columns"));
	global_context->addTable("system", "functions", StorageSystemFunctions::create("functions"));
	global_context->addTable("system", "clusters", StorageSystemClusters::create("clusters", *global_context));

	if (has_zookeeper)
		global_context->addTable("system", "zookeeper", StorageSystemZooKeeper::create("zookeeper"));

	global_context->setCurrentDatabase(config().getString("default_database", "default"));

	SCOPE_EXIT(
		LOG_DEBUG(log, "Closed all connections.");

		/** Попросим завершить фоновую работу у всех движков таблиц.
		  * Это важно делать заранее, не в деструкторе Context-а, так как
		  *  движки таблиц могут при уничтожении всё ещё пользоваться Context-ом.
		  */
		LOG_INFO(log, "Shutting down storages.");
		global_context->shutdown();
		LOG_DEBUG(log, "Shutted down storages.");

		/** Явно уничтожаем контекст - это удобнее, чем в деструкторе Server-а, так как ещё доступен логгер.
		  * В этот момент никто больше не должен владеть shared-частью контекста.
		  */
		global_context.reset();

		LOG_DEBUG(log, "Destroyed global context.");
	);

	{
		const auto profile_events_transmitter = config().getBool("use_graphite", true)
			? std::make_unique<ProfileEventsTransmitter>()
			: nullptr;

		const std::string listen_host = config().getString("listen_host", "::");

		Poco::Timespan keep_alive_timeout(config().getInt("keep_alive_timeout", 10), 0);

		Poco::ThreadPool server_pool(3, config().getInt("max_connections", 1024));
		Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
		http_params->setTimeout(settings.receive_timeout);
		http_params->setKeepAliveTimeout(keep_alive_timeout);

		/// HTTP
		Poco::Net::ServerSocket http_socket(Poco::Net::SocketAddress(listen_host, config().getInt("http_port")));
		http_socket.setReceiveTimeout(settings.receive_timeout);
		http_socket.setSendTimeout(settings.send_timeout);
		Poco::Net::HTTPServer http_server(
			new HTTPRequestHandlerFactory<HTTPHandler>(*this, "HTTPHandler-factory"),
			server_pool,
			http_socket,
			http_params);

		/// TCP
		Poco::Net::ServerSocket tcp_socket(Poco::Net::SocketAddress(listen_host, config().getInt("tcp_port")));
		tcp_socket.setReceiveTimeout(settings.receive_timeout);
		tcp_socket.setSendTimeout(settings.send_timeout);
		Poco::Net::TCPServer tcp_server(
			new TCPConnectionFactory(*this),
			server_pool,
			tcp_socket,
			new Poco::Net::TCPServerParams);

		/// Interserver IO HTTP
		Poco::SharedPtr<Poco::Net::HTTPServer> interserver_io_http_server;
		if (config().has("interserver_http_port"))
		{
			Poco::Net::ServerSocket interserver_io_http_socket(Poco::Net::SocketAddress(listen_host, config().getInt("interserver_http_port")));
			interserver_io_http_socket.setReceiveTimeout(settings.receive_timeout);
			interserver_io_http_socket.setSendTimeout(settings.send_timeout);
			interserver_io_http_server = new Poco::Net::HTTPServer(
				new HTTPRequestHandlerFactory<InterserverIOHTTPHandler>(*this, "InterserverIOHTTPHandler-factory"),
				server_pool,
				interserver_io_http_socket,
				http_params);
		}

		/// OLAP HTTP
		Poco::SharedPtr<Poco::Net::HTTPServer> olap_http_server;
		bool use_olap_server = config().has("olap_compatibility.port");
		if (use_olap_server)
		{
			olap_parser.reset(new OLAP::QueryParser());
			olap_converter.reset(new OLAP::QueryConverter(config()));

			Poco::Net::ServerSocket olap_http_socket(Poco::Net::SocketAddress(listen_host, config().getInt("olap_compatibility.port")));
			olap_http_socket.setReceiveTimeout(settings.receive_timeout);
			olap_http_socket.setSendTimeout(settings.send_timeout);
			olap_http_server = new Poco::Net::HTTPServer(
				new HTTPRequestHandlerFactory<OLAPHTTPHandler>(*this, "OLAPHTTPHandler-factory"),
				server_pool,
				olap_http_socket,
				http_params);
		}

		http_server.start();
		tcp_server.start();
		if (interserver_io_http_server)
			interserver_io_http_server->start();
		if (olap_http_server)
			olap_http_server->start();

		LOG_INFO(log, "Ready for connections.");

		SCOPE_EXIT(
			LOG_DEBUG(log, "Received termination signal. Waiting for current connections to close.");

			users_config_reloader.reset();

			is_cancelled = true;

			http_server.stop();
			tcp_server.stop();
			if (use_olap_server)
				olap_http_server->stop();
		);

		/// try to load dictionaries immediately, throw on error and die
		try
		{
			if (!config().getBool("dictionaries_lazy_load", true))
			{
				global_context->tryCreateDictionaries();
				global_context->tryCreateExternalDictionaries();
			}

			waitForTerminationRequest();
		}
		catch (...)
		{
			LOG_ERROR(log, "Caught exception while loading dictionaries.");
			throw;
		}
	}

	return Application::EXIT_OK;
}

}


YANDEX_APP_SERVER_MAIN(DB::Server);
