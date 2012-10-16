#pragma once

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Client/Connection.h>


namespace DB
{

/** Позволяет выполнить запрос (SELECT) на удалённом сервере и получить результат.
  */
class RemoteBlockInputStream : public IProfilingBlockInputStream
{
public:
	RemoteBlockInputStream(Connection & connection_, const String & query_, QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete)
		: connection(connection_), query(query_), stage(stage_), sent_query(false), finished(false), cancelled(false),
		log(&Logger::get("RemoteBlockInputStream (" + connection.getServerAddress() + ")"))
	{
	}
		
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
					packet.exception->rethrow();
					break;

				case Protocol::Server::EndOfStream:
					finished = true;
					return Block();

				case Protocol::Server::Progress:
					if (progress_callback)
						progress_callback(packet.progress.rows, packet.progress.bytes);

					if (is_cancelled_callback && is_cancelled_callback())
					{
						LOG_TRACE(log, "Cancelling query");

						/// Если запрошено прервать запрос - попросим удалённый сервер тоже прервать запрос.
						cancelled = true;
						connection.sendCancel();
					}
					
					break;

				default:
					throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
			}
		}
	}

	String getName() const { return "RemoteBlockInputStream"; }

	BlockInputStreamPtr clone() { return new RemoteBlockInputStream(connection, query, stage); }

	/** Отменяем умолчальное уведомление о прогрессе,
	  * так как колбэк прогресса вызывается самостоятельно.
	  */
	void progress(Block & block) {}

    ~RemoteBlockInputStream()
	{
		/// Если ещё прочитали не все данные, но они больше не нужны, то отправим просьбу прервать выполнение запроса.
		if (sent_query && !finished)
		{
			if (!cancelled)
			{
				LOG_TRACE(log, "Cancelling query because enough data has been read");
				
				cancelled = true;
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
						if (!std::uncaught_exception())
							packet.exception->rethrow();
						break;

					default:
						if (!std::uncaught_exception())
							throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
				}
			}
		}
	}

private:
	Connection & connection;
	const String query;
	QueryProcessingStage::Enum stage;

	bool sent_query;
	bool finished;
	bool cancelled;

	Logger * log;
};

}
