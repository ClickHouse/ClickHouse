#pragma once

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Common/VirtualColumnUtils.h>

#include <DB/Client/ConnectionPool.h>


namespace DB
{

/** Позволяет выполнить запрос (SELECT) на удалённом сервере и получить результат.
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
		const Context & context = Context{})
		: connection(&connection_), query(query_), external_tables(external_tables_), stage(stage_), context(context)
	{
		init(settings_);
	}

	/// Принимает готовое соединение. Захватывает владение соединением из пула.
	RemoteBlockInputStream(ConnectionPool::Entry & pool_entry_, const String & query_, const Settings * settings_,
		const Tables & external_tables_ = Tables(), QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context = Context{})
		: pool_entry(pool_entry_), connection(&*pool_entry_), query(query_),
		  external_tables(external_tables_), stage(stage_), context(context)
	{
		init(settings_);
	}

	/// Принимает пул, из которого нужно будет достать соединение.
	RemoteBlockInputStream(IConnectionPool * pool_, const String & query_, const Settings * settings_,
		const Tables & external_tables_ = Tables(), QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete,
		const Context & context = Context{})
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
			LOG_TRACE(log, "(" + connection->getServerAddress() + ") Cancelling query");

			/// Если запрошено прервать запрос - попросим удалённый сервер тоже прервать запрос.
			connection->sendCancel();
			was_cancelled = true;
		}
	}


	~RemoteBlockInputStream() override
	{
		/** Если прервались в середине цикла общения с сервером, то закрываем соединение,
		  *  чтобы оно не осталось висеть в рассихронизированном состоянии.
		  */
		if (sent_query && !finished)
			connection->disconnect();
	}

protected:
	/// Отправить на удаленные сервера все временные таблицы
	void sendExternalTables()
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
		connection->sendExternalTablesData(res);
	}

	Block readImpl() override
	{
		if (!sent_query)
		{
			/// Если надо - достаём соединение из пула.
			if (pool)
			{
				pool_entry = pool->get(send_settings ? &settings : nullptr);
				connection = &*pool_entry;
			}

			connection->sendQuery(query, "", stage, send_settings ? &settings : nullptr, true);
			sendExternalTables();
			sent_query = true;
		}

		while (true)
		{
			Connection::Packet packet = connection->receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
					/// Если блок не пуст и не является заголовочным блоком
					if (packet.block && packet.block.rows() > 0)
						return packet.block;
					break;	/// Если блок пустой - получим другие пакеты до EndOfStream.

				case Protocol::Server::Exception:
					got_exception_from_server = true;
					packet.exception->rethrow();
					break;

				case Protocol::Server::EndOfStream:
					finished = true;
					return Block();

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
			LOG_TRACE(log, "(" + connection->getServerAddress() + ") Cancelling query because enough data has been read");

			was_cancelled = true;
			connection->sendCancel();
		}

		/// Получим оставшиеся пакеты, чтобы не было рассинхронизации в соединении с сервером.
		while (true)
		{
			Connection::Packet packet = connection->receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
				case Protocol::Server::Progress:
				case Protocol::Server::ProfileInfo:
				case Protocol::Server::Totals:
				case Protocol::Server::Extremes:
					break;

				case Protocol::Server::EndOfStream:
					return;

				case Protocol::Server::Exception:
					got_exception_from_server = true;
					packet.exception->rethrow();
					break;

				default:
					throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
			}
		}

		finished = true;
	}

private:
	IConnectionPool * pool = nullptr;
	ConnectionPool::Entry pool_entry;
	Connection * connection = nullptr;

	const String query;
	bool send_settings;
	Settings settings;
	/// Временные таблицы, которые необходимо переслать на удаленные сервера.
	Tables external_tables;
	QueryProcessingStage::Enum stage;
	Context context;

	/// Отправили запрос (это делается перед получением первого блока).
	bool sent_query = false;

	/** Получили все данные от сервера, до пакета EndOfStream.
	  * Если при уничтожении объекта, ещё не все данные считаны,
	  *  то для того, чтобы не было рассинхронизации, на сервер отправляется просьба прервать выполнение запроса,
	  *  и после этого считываются все пакеты до EndOfStream.
	  */
	bool finished = false;

	/** На сервер была отправлена просьба прервать выполенение запроса, так как данные больше не нужны.
	  * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT),
	  *  или если на стороне клиента произошло исключение.
	  */
	bool was_cancelled = false;

	/// С сервера было получено исключение. В этом случае получать больше пакетов или просить прервать запрос не нужно.
	bool got_exception_from_server = false;

	Logger * log = &Logger::get("RemoteBlockInputStream");
};

}
