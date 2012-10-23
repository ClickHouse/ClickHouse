#pragma once

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Client/ConnectionPool.h>


namespace DB
{

/** Позволяет выполнить запрос (SELECT) на удалённом сервере и получить результат.
  */
class RemoteBlockInputStream : public IProfilingBlockInputStream
{
public:
	RemoteBlockInputStream(Connection & connection_, const String & query_,
		QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete)
		: connection(connection_), query(query_), stage(stage_),
		sent_query(false), finished(false), was_cancelled(false), got_exception_from_server(false),
		log(&Logger::get("RemoteBlockInputStream (" + connection.getServerAddress() + ")"))
	{
	}

	/// Захватывает владение соединением из пула.
	RemoteBlockInputStream(ConnectionPool::Entry pool_entry_, const String & query_,
		QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete)
		: pool_entry(pool_entry_), connection(*pool_entry), query(query_), stage(stage_),
		sent_query(false), finished(false), was_cancelled(false), got_exception_from_server(false),
		log(&Logger::get("RemoteBlockInputStream (" + connection.getServerAddress() + ")"))
	{
	}


	String getName() const { return "RemoteBlockInputStream"; }

	BlockInputStreamPtr clone()
	{
		return pool_entry.isNull()
			? new RemoteBlockInputStream(connection, query, stage)
			: new RemoteBlockInputStream(pool_entry, query, stage);
	}


	/** Отменяем умолчальное уведомление о прогрессе,
	  * так как колбэк прогресса вызывается самостоятельно.
	  */
	void progress(Block & block) {}


	void cancel()
	{
		if (sent_query && !was_cancelled && !finished && !got_exception_from_server)
		{
			LOG_TRACE(log, "Cancelling query");

			/// Если запрошено прервать запрос - попросим удалённый сервер тоже прервать запрос.
			is_cancelled = true;
			was_cancelled = true;
			connection.sendCancel();
		}
	}


    ~RemoteBlockInputStream()
	{
		/** Если одно из:
		  *   - ничего не начинали делать;
		  *   - получили все пакеты до EndOfStream;
		  *   - получили с сервера эксепшен;
		  *   - объект уничтожается из-за эксепшена;
		  * - то больше читать ничего не нужно.
		  */
		if (!sent_query || finished || got_exception_from_server || std::uncaught_exception())
			return;

		/** Если ещё прочитали не все данные, но они больше не нужны.
		  * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT).
		  */

		/// Отправим просьбу прервать выполнение запроса, если ещё не отправляли.
		if (!was_cancelled)
		{
			LOG_TRACE(log, "Cancelling query because enough data has been read");
				
			was_cancelled = true;
			connection.sendCancel();
		}

		/// Получим оставшиеся пакеты, чтобы не было рассинхронизации в соединении с сервером.
		while (true)
		{
			Connection::Packet packet = connection.receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
				case Protocol::Server::Progress:
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
	}

protected:
	Block readImpl()
	{
		if (!sent_query)
		{
			connection.sendQuery(query, 1, stage);
			sent_query = true;
		}

		while (true)
		{
			Connection::Packet packet = connection.receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
					if (packet.block)
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
					if (progress_callback)
						progress_callback(packet.progress.rows, packet.progress.bytes);

					if (!was_cancelled && !finished && isCancelled())
						cancel();

					break;

				default:
					throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
			}
		}
	}

private:
	/// Используется, если нужно владеть соединением из пула
	ConnectionPool::Entry pool_entry;
	
	Connection & connection;

	const String query;
	QueryProcessingStage::Enum stage;

	/// Отправили запрос (это делается перед получением первого блока).
	bool sent_query;
	
	/** Получили все данные от сервера, до пакета EndOfStream.
	  * Если при уничтожении объекта, ещё не все данные считаны,
	  *  то для того, чтобы не было рассинхронизации, на сервер отправляется просьба прервать выполнение запроса,
	  *  и после этого считываются все пакеты до EndOfStream.
	  */
	bool finished;
	
	/** На сервер была отправлена просьба прервать выполенение запроса, так как данные больше не нужны.
	  * Это может быть из-за того, что данных достаточно (например, при использовании LIMIT),
	  *  или если на стороне клиента произошло исключение.
	  */
	bool was_cancelled;

	/// С сервера было получено исключение. В этом случае получать больше пакетов или просить прервать запрос не нужно.
	bool got_exception_from_server;

	Logger * log;
};

}
