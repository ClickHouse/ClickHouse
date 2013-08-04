#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Client/Connection.h>


namespace DB
{

/** Позволяет выполнить запрос INSERT на удалённом сервере и отправить данные.
  */
class RemoteBlockOutputStream : public IBlockOutputStream
{
public:
	RemoteBlockOutputStream(Connection & connection_, const String & query_)
		: connection(connection_), query(query_)
	{
	}


	/** Отправляет запрос и получает блок-пример, описывающий структуру таблицы.
	  * Он нужен, чтобы знать, какие блоки передавать в метод write.
	  * Вызывайте только перед write.
	  */
	Block sendQueryAndGetSampleBlock()
	{
		connection.sendQuery(query);
		sent_query = true;

		Connection::Packet packet = connection.receivePacket();

		if (Protocol::Server::Data == packet.type)
			return packet.block;
		else if (Protocol::Server::Exception == packet.type)
		{
			packet.exception->rethrow();
			return Block();
		}
		else
			throw Exception("Unexpected packet from server (expected Data or Exception, got "
				+ String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
	}
	

	void write(const Block & block)
	{
		if (!sent_query)
			sendQueryAndGetSampleBlock();

		connection.sendData(block);
	}


	void writeSuffix()
	{
		/// Пустой блок означает конец данных.
		connection.sendData(Block());

		/// Получаем пакет EndOfStream.
		Connection::Packet packet = connection.receivePacket();

		if (Protocol::Server::EndOfStream == packet.type)
		{
			/// Ничего.
		}
		else if (Protocol::Server::Exception == packet.type)
			packet.exception->rethrow();
		else
			throw Exception("Unexpected packet from server (expected EndOfStream or Exception, got "
				+ String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
	}

private:
	Connection & connection;
	String query;

	bool sent_query;
};

}
