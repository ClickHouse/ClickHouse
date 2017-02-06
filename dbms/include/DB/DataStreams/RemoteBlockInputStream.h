#pragma once

#include <common/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Common/Throttler.h>
#include <DB/Interpreters/Context.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/Client/MultiplexedConnections.h>
#include <DB/Interpreters/Cluster.h>


namespace DB
{

/** Позволяет выполнить запрос на удалённых репликах одного шарда и получить результат.
  */
class RemoteBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// Принимает готовое соединение.
	RemoteBlockInputStream(Connection & connection_, const String & query_, const Settings * settings_,
		ThrottlerPtr throttler_ = nullptr, const Tables & external_tables_ = Tables(),
		QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context_ = getDefaultContext());

	/// Принимает готовое соединение. Захватывает владение соединением из пула.
	RemoteBlockInputStream(ConnectionPool::Entry & pool_entry_, const String & query_, const Settings * settings_,
		ThrottlerPtr throttler_ = nullptr, const Tables & external_tables_ = Tables(),
		QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context_ = getDefaultContext());

	/// Принимает пул, из которого нужно будет достать одно или несколько соединений.
	RemoteBlockInputStream(ConnectionPoolPtr & pool_, const String & query_, const Settings * settings_,
		ThrottlerPtr throttler_ = nullptr, const Tables & external_tables_ = Tables(),
		QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context_ = getDefaultContext());

	/// Принимает пулы - один для каждого шарда, из которых нужно будет достать одно или несколько соединений.
	RemoteBlockInputStream(ConnectionPoolsPtr & pools_, const String & query_, const Settings * settings_,
		ThrottlerPtr throttler_ = nullptr, const Tables & external_tables_ = Tables(),
		QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context_ = getDefaultContext());

	~RemoteBlockInputStream() override;

	/// Specify how we allocate connections on a shard.
	void setPoolMode(PoolMode pool_mode_);

	/// Кроме блоков, получить информацию о блоках.
	void appendExtraInfo();

	/// Отправляет запрос (инициирует вычисления) раньше, чем read.
	void readPrefix() override;

	/** Отменяем умолчальное уведомление о прогрессе,
	  * так как колбэк прогресса вызывается самостоятельно.
	  */
	void progress(const Progress & value) override {}

	void cancel() override;

	String getName() const override { return "Remote"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

	BlockExtraInfo getBlockExtraInfo() const override
	{
		return multiplexed_connections->getBlockExtraInfo();
	}

protected:
	/// Отправить на удаленные серверы все временные таблицы.
	void sendExternalTables();

	Block readImpl() override;

	void readSuffixImpl() override;

	/// Создать объект для общения с репликами одного шарда, на которых должен выполниться запрос.
	void createMultiplexedConnections();

	/// Возвращает true, если запрос отправлен.
	bool isQueryPending() const;

	/// Возвращает true, если исключение было выкинуто.
	bool hasThrownException() const;

private:
	void init(const Settings * settings_);

	void sendQuery();

	/// Отправить запрос на отмену всех соединений к репликам, если такой запрос ещё не был отправлен.
	void tryCancel(const char * reason);

	/// ITable::read requires a Context, therefore we should create one if the user can't supply it
	static Context & getDefaultContext()
	{
		static Context instance;
		return instance;
	}

private:
	/// Готовое соединение.
	ConnectionPool::Entry pool_entry;
	Connection * connection = nullptr;

	/// Пул соединений одного шарда.
	ConnectionPoolPtr pool = nullptr;

	/// Пулы соединений одного или нескольких шардов.
	ConnectionPoolsPtr pools;

	std::unique_ptr<MultiplexedConnections> multiplexed_connections;

	const String query;
	bool send_settings;
	Settings settings;
	/// Если не nullptr, то используется, чтобы ограничить сетевой трафик.
	ThrottlerPtr throttler;
	/// Временные таблицы, которые необходимо переслать на удаленные сервера.
	Tables external_tables;
	QueryProcessingStage::Enum stage;
	Context context;

	/// Потоки для чтения из временных таблиц - для последующей отправки данных на удалённые серверы для GLOBAL-подзапросов.
	std::vector<ExternalTablesData> external_tables_data;
	std::mutex external_tables_mutex;

	/// Установили соединения с репликами, но ещё не отправили запрос.
	std::atomic<bool> established { false };

	/// Отправили запрос (это делается перед получением первого блока).
	std::atomic<bool> sent_query { false };

	/** Получили все данные от всех реплик, до пакета EndOfStream.
	  * Если при уничтожении объекта, ещё не все данные считаны,
	  *  то для того, чтобы не было рассинхронизации, на реплики отправляются просьбы прервать выполнение запроса,
	  *  и после этого считываются все пакеты до EndOfStream.
	  */
	std::atomic<bool> finished { false };

	/** На каждую реплику была отправлена просьба прервать выполнение запроса, так как данные больше не нужны.
	  * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT),
	  *  или если на стороне клиента произошло исключение.
	  */
	std::atomic<bool> was_cancelled { false };

	/** С одной репилки было получено исключение. В этом случае получать больше пакетов или
	  * просить прервать запрос на этой реплике не нужно.
	  */
	std::atomic<bool> got_exception_from_replica { false };

	/** С одной реплики был получен неизвестный пакет. В этом случае получать больше пакетов или
	  * просить прервать запрос на этой реплике не нужно.
	  */
	std::atomic<bool> got_unknown_packet_from_replica { false };

	bool append_extra_info = false;
	PoolMode pool_mode = PoolMode::GET_MANY;

	Logger * log = &Logger::get("RemoteBlockInputStream");
};

}
