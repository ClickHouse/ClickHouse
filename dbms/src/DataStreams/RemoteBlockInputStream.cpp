#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Common/VirtualColumnUtils.h>

namespace DB
{

RemoteBlockInputStream::RemoteBlockInputStream(Connection & connection_, const String & query_,
	const Settings * settings_, ThrottlerPtr throttler_, const Tables & external_tables_,
	QueryProcessingStage::Enum stage_, const Context & context_)
	: connection(&connection_), query(query_), throttler(throttler_), external_tables(external_tables_),
	stage(stage_), context(context_)
{
	init(settings_);
}

RemoteBlockInputStream::RemoteBlockInputStream(ConnectionPool::Entry & pool_entry_, const String & query_,
	const Settings * settings_, ThrottlerPtr throttler_, const Tables & external_tables_,
	QueryProcessingStage::Enum stage_,	const Context & context_)
	: pool_entry(pool_entry_), connection(&*pool_entry_), query(query_), throttler(throttler_),
		external_tables(external_tables_), stage(stage_), context(context_)
{
	init(settings_);
}

RemoteBlockInputStream::RemoteBlockInputStream(IConnectionPool * pool_, const String & query_,
	const Settings * settings_, ThrottlerPtr throttler_, const Tables & external_tables_,
	QueryProcessingStage::Enum stage_, const Context & context_)
	: pool(pool_), query(query_), throttler(throttler_), external_tables(external_tables_),
	stage(stage_), context(context_)
{
	init(settings_);
}

RemoteBlockInputStream::RemoteBlockInputStream(ConnectionPoolsPtr & pools_, const String & query_,
	const Settings * settings_, ThrottlerPtr throttler_, const Tables & external_tables_,
	QueryProcessingStage::Enum stage_, const Context & context_)
	: pools(pools_), query(query_), throttler(throttler_), external_tables(external_tables_),
	stage(stage_), context(context_)
{
	init(settings_);
}

RemoteBlockInputStream::~RemoteBlockInputStream()
{
	/** Если прервались в середине цикла общения с репликами, то прервываем
	  * все соединения, затем читаем и пропускаем оставшиеся пакеты чтобы
	  * эти соединения не остались висеть в рассихронизированном состоянии.
	  */
	if (established || isQueryPending())
		multiplexed_connections->disconnect();
}

void RemoteBlockInputStream::doBroadcast()
{
	do_broadcast = true;
}

void RemoteBlockInputStream::appendExtraInfo()
{
	append_extra_info = true;
}

void RemoteBlockInputStream::readPrefix()
{
	if (!sent_query)
		sendQuery();
}

void RemoteBlockInputStream::cancel()
{
	bool old_val = false;
	if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
		return;

	{
		std::lock_guard<std::mutex> lock(external_tables_mutex);

		/// Останавливаем отправку внешних данных.
		for (auto & vec : external_tables_data)
			for (auto & elem : vec)
				if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(elem.first.get()))
					stream->cancel();
	}

	if (!isQueryPending() || hasThrownException())
		return;

	tryCancel("Cancelling query");
}

void RemoteBlockInputStream::sendExternalTables()
{
	size_t count = multiplexed_connections->size();

	{
		std::lock_guard<std::mutex> lock(external_tables_mutex);

		external_tables_data.reserve(count);

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
			external_tables_data.push_back(std::move(res));
		}
	}

	multiplexed_connections->sendExternalTablesData(external_tables_data);
}

Block RemoteBlockInputStream::readImpl()
{
	if (!sent_query)
	{
		sendQuery();

		if (settings.skip_unavailable_shards && (0 == multiplexed_connections->size()))
			return {};
	}

	while (true)
	{
		if (isCancelled())
			return Block();

		Connection::Packet packet = multiplexed_connections->receivePacket();

		switch (packet.type)
		{
			case Protocol::Server::Data:
				/// Если блок не пуст и не является заголовочным блоком
				if (packet.block && (packet.block.rows() > 0))
					return packet.block;
				break;	/// Если блок пуст - получим другие пакеты до EndOfStream.

			case Protocol::Server::Exception:
				got_exception_from_replica = true;
				packet.exception->rethrow();
				break;

			case Protocol::Server::EndOfStream:
				if (!multiplexed_connections->hasActiveConnections())
				{
					finished = true;
					return Block();
				}
				break;

			case Protocol::Server::Progress:
				/** Используем прогресс с удалённого сервера.
				  * В том числе, запишем его в ProcessList,
				  * и будем использовать его для проверки
				  * ограничений (например, минимальная скорость выполнения запроса)
				  * и квот (например, на количество строчек для чтения).
				  */
				progressImpl(packet.progress);
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
				got_unknown_packet_from_replica = true;
				throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
		}
	}
}

void RemoteBlockInputStream::readSuffixImpl()
{
	/** Если одно из:
	  * - ничего не начинали делать;
	  * - получили все пакеты до EndOfStream;
	  * - получили с одной реплики эксепшен;
	  * - получили с одной реплики неизвестный пакет;
	  * то больше читать ничего не нужно.
	  */
	if (!isQueryPending() || hasThrownException())
		return;

	/** Если ещё прочитали не все данные, но они больше не нужны.
	  * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT).
	  */

	/// Отправим просьбу прервать выполнение запроса, если ещё не отправляли.
	tryCancel("Cancelling query because enough data has been read");

	/// Получим оставшиеся пакеты, чтобы не было рассинхронизации в соединениях с репликами.
	Connection::Packet packet = multiplexed_connections->drain();
	switch (packet.type)
	{
		case Protocol::Server::EndOfStream:
			finished = true;
			break;

		case Protocol::Server::Exception:
			got_exception_from_replica = true;
			packet.exception->rethrow();
			break;

		default:
			got_unknown_packet_from_replica = true;
			throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
	}
}

void RemoteBlockInputStream::createMultiplexedConnections()
{
	Settings * multiplexed_connections_settings = send_settings ? &settings : nullptr;
	if (connection != nullptr)
		multiplexed_connections = std::make_unique<MultiplexedConnections>(connection, multiplexed_connections_settings, throttler);
	else if (pool != nullptr)
		multiplexed_connections = std::make_unique<MultiplexedConnections>(pool, multiplexed_connections_settings, throttler,
			append_extra_info, do_broadcast);
	else if (!pools.isNull())
		multiplexed_connections = std::make_unique<MultiplexedConnections>(*pools, multiplexed_connections_settings, throttler,
			append_extra_info, do_broadcast);
	else
		throw Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
}

void RemoteBlockInputStream::init(const Settings * settings_)
{
	if (settings_)
	{
		send_settings = true;
		settings = *settings_;
	}
	else
		send_settings = false;
}

void RemoteBlockInputStream::sendQuery()
{
	createMultiplexedConnections();

	if (settings.skip_unavailable_shards && 0 == multiplexed_connections->size())
		return;

	established = true;

	multiplexed_connections->sendQuery(query, "", stage, true);

	established = false;
	sent_query = true;

	sendExternalTables();
}

void RemoteBlockInputStream::tryCancel(const char * reason)
{
	bool old_val = false;
	if (!was_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
		return;

	LOG_TRACE(log, "(" << multiplexed_connections->dumpAddresses() << ") " << reason);
	multiplexed_connections->sendCancel();
}

bool RemoteBlockInputStream::isQueryPending() const
{
	return sent_query && !finished;
}

bool RemoteBlockInputStream::hasThrownException() const
{
	return got_exception_from_replica || got_unknown_packet_from_replica;
}

}
