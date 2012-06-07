#pragma once

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
		: connection(connection_), query(query_), stage(stage_), sent_query(false)
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
					return Block();

				case Protocol::Server::Progress:
					if (progress_callback)
						progress_callback(packet.progress.rows, packet.progress.bytes);
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

private:
	Connection & connection;
	const String query;
	QueryProcessingStage::Enum stage;

	bool sent_query;
};

}
