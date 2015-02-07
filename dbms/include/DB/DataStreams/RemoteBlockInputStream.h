#pragma once

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/Interpreters/Context.h>

#include <DB/Client/ConnectionPool.h>
#include <DB/Client/ParallelReplicas.h>


namespace DB
{

/** Позволяет выполнить запрос (SELECT) на удалённых репликах одного шарда и получить результат.
  */
class RemoteBlockInputStream : public IProfilingBlockInputStream
{
private:
	void init(const Settings * settings_)
	{
		if (settings_)
		{
			send_settings = true;
			settings = *settings_;
		}
		else
			send_settings = false;
	}

public:
	/// Принимает готовое соединение.
	RemoteBlockInputStream(Connection & connection_, const String & query_, const Settings * settings_,
		const Tables & external_tables_ = Tables(), QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context = getDefaultContext())
		: connection(&connection_), query(query_), external_tables(external_tables_), stage(stage_), context(context)
	{
		init(settings_);
	}

	/// Принимает готовое соединение. Захватывает владение соединением из пула.
	RemoteBlockInputStream(ConnectionPool::Entry & pool_entry_, const String & query_, const Settings * settings_,
		const Tables & external_tables_ = Tables(), QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context = getDefaultContext())
		: pool_entry(pool_entry_), connection(&*pool_entry_), query(query_),
		  external_tables(external_tables_), stage(stage_), context(context)
	{
		init(settings_);
	}

	/// Принимает пул, из которого нужно будет достать одно или несколько соединений.
	RemoteBlockInputStream(IConnectionPool * pool_, const String & query_, const Settings * settings_,
		const Tables & external_tables_ = Tables(), QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context = getDefaultContext())
		: pool(pool_), query(query_), external_tables(external_tables_), stage(stage_), context(context)
	{
		init(settings_);
	}


	String getName() const override { return "RemoteBlockInputStream"; }


	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}


	/** Отменяем умолчальное уведомление о прогрессе,
	  * так как колбэк прогресса вызывается самостоятельно.
	  */
	void progress(const Progress & value) override {}


	void cancel() override
	{
		if (!__sync_bool_compare_and_swap(&is_cancelled, false, true))
			return;

		if (sent_query && !was_cancelled && !finished && !got_exception_from_server)
		{
			std::string addresses = parallel_replicas->dumpAddresses();
			LOG_TRACE(log, "(" + addresses + ") Cancelling query");

			/// Если запрошено прервать запрос - попросим удалённые реплики тоже прервать запрос.
			parallel_replicas->sendCancel();
			was_cancelled = true;
		}
	}


	~RemoteBlockInputStream() override
	{
		/** Если прервались в середине цикла общения с репликами, то закрываем соединения,
		  *  чтобы они не остались висеть в рассихронизированном состоянии.
		  */
		if (sent_query && !finished)
			parallel_replicas->disconnect();
	}

protected:
	/// Отправить на удаленные реплики все временные таблицы
	void sendExternalTables()
	{
		size_t count = parallel_replicas->size();

		std::vector<ExternalTablesData> instances;
		instances.reserve(count);

		for (size_t i = 0; i < count; ++i)
		{
			ExternalTablesData res;
			for (const auto & table : external_tables)
			{
				StoragePtr cur = table.second;
				QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;
				DB::BlockInputStreams input = cur->read(cur->getColumnNamesList(), ASTPtr(), context, settings,
					stage, DEFAULT_BLOCK_SIZE, 1);
				if (input.size() == 0)
					res.push_back(std::make_pair(new OneBlockInputStream(cur->getSampleBlock()), table.first));
				else
					res.push_back(std::make_pair(input[0], table.first));
			}
			instances.push_back(std::move(res));
		}

		parallel_replicas->sendExternalTablesData(instances);
	}

	Block readImpl() override
	{
		if (!sent_query)
		{
			createParallelReplicas();
			parallel_replicas->sendQuery(query, "", stage, true);
			sendExternalTables();
			sent_query = true;
		}

		while (true)
		{
			Connection::Packet packet = parallel_replicas->receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
					/// Если блок не пуст и не является заголовочным блоком
					if (packet.block && packet.block.rows() > 0)
						return packet.block;
					break;	/// Если блок пустой - получим другие пакеты до EndOfStream.

				case Protocol::Server::Exception:
					got_exception_from_server = true;
					abort();
					packet.exception->rethrow();
					break;

				case Protocol::Server::EndOfStream:
					if (!parallel_replicas->hasActiveReplicas())
					{
						finished = true;
						return Block();
					}
					break;

				case Protocol::Server::Progress:
					/** Используем прогресс с удалённого сервера.
					  * В том числе, запишем его в ProcessList,
					  *  и будем использовать его для проверки
					  *  ограничений (например, минимальная скорость выполнения запроса)
					  *  и квот (например, на количество строчек для чтения).
					  */
					progressImpl(packet.progress);

					if (!was_cancelled && !finished && isCancelled())
						cancel();

					break;

				case Protocol::Server::ProfileInfo:
					info = packet.profile_info;
					break;

				case Protocol::Server::Totals:
					totals = packet.block;
					break;

				case Protocol::Server::Extremes:
					extremes = packet.block;
					break;

				default:
					abort();
					throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
			}
		}
	}

	void readSuffixImpl() override
	{
		/** Если одно из:
		 *   - ничего не начинали делать;
		 *   - получили все пакеты до EndOfStream;
		 *   - получили с сервера эксепшен;
		 * - то больше читать ничего не нужно.
		 */
		if (!sent_query || finished || got_exception_from_server)
			return;

		/** Если ещё прочитали не все данные, но они больше не нужны.
		 * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT).
		 */

		/// Отправим просьбу прервать выполнение запроса, если ещё не отправляли.
		if (!was_cancelled)
		{
			std::string addresses = parallel_replicas->dumpAddresses();
			LOG_TRACE(log, "(" + addresses + ") Cancelling query because enough data has been read");

			was_cancelled = true;
			parallel_replicas->sendCancel();
		}

		/// Получим оставшиеся пакеты, чтобы не было рассинхронизации в соединениях с репликами.
		Connection::Packet packet = parallel_replicas->drain();
		switch (packet.type)
		{
			case Protocol::Server::EndOfStream:
				finished = true;
				break;

			case Protocol::Server::Exception:
				got_exception_from_server = true;
				packet.exception->rethrow();
				break;

			default:
				throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
		}
	}

	/// Создать структуру для общения с репликами одного шарда, на которых должен выполниться запрос.
	void createParallelReplicas()
	{
		Settings * parallel_replicas_settings = send_settings ? &settings : nullptr;
		if (connection != nullptr)
			parallel_replicas = ext::make_unique<ParallelReplicas>(connection, parallel_replicas_settings);
		else
			parallel_replicas = ext::make_unique<ParallelReplicas>(pool, parallel_replicas_settings);
	}

	/// Если был получен пакет типа Exception или неизвестного типа, отменить запросы на всех
	/// репликах. Читать и пропускать все оставшиеся пакеты до EndOfStream или Exception, чтобы
	/// не было рассинхронизации в соединениях с репликами.
	void abort()
	{
		std::string addresses = parallel_replicas->dumpAddresses();
		LOG_TRACE(log, "(" + addresses + ") Aborting query");

		parallel_replicas->sendCancel();
		(void) parallel_replicas->drain();
	}

private:
	IConnectionPool * pool = nullptr;

	ConnectionPool::Entry pool_entry;
	Connection * connection = nullptr;
	std::unique_ptr<ParallelReplicas> parallel_replicas;

	const String query;
	bool send_settings;
	Settings settings;
	/// Временные таблицы, которые необходимо переслать на удаленные сервера.
	Tables external_tables;
	QueryProcessingStage::Enum stage;
	Context context;

	/// Отправили запрос (это делается перед получением первого блока).
	bool sent_query = false;

	/** Получили все данные от всех реплик, до пакета EndOfStream.
	  * Если при уничтожении объекта, ещё не все данные считаны,
	  *  то для того, чтобы не было рассинхронизации, на реплики отправляются просьбы прервать выполнение запроса,
	  *  и после этого считываются все пакеты до EndOfStream.
	  */
	bool finished = false;

	/** На каждую реплику была отправлена просьба прервать выполенение запроса, так как данные больше не нужны.
	  * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT),
	  *  или если на стороне клиента произошло исключение.
	  */
	bool was_cancelled = false;

	/// С одной репилки было получено исключение. В этом случае получать больше пакетов или просить прервать запрос не нужно.
	bool got_exception_from_server = false;

	Logger * log = &Logger::get("RemoteBlockInputStream");

	/// ITable::read requires a Context, therefore we should create one if the user can't supply it
	static Context & getDefaultContext()
	{
		static Context instance;
		return instance;
	}
};

}
