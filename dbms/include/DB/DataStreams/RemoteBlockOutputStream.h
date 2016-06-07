#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Client/Connection.h>
#include <common/logger_useful.h>

#include <DB/Common/NetException.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int UNEXPECTED_PACKET_FROM_SERVER;
	extern const int LOGICAL_ERROR;
}


/** Позволяет выполнить запрос INSERT на удалённом сервере и отправить данные.
  */
class RemoteBlockOutputStream : public IBlockOutputStream
{
public:
	RemoteBlockOutputStream(Connection & connection_, const String & query_, const Settings * settings_ = nullptr)
		: connection(connection_), query(query_), settings(settings_)
	{
	}


	/// Можно вызывать после writePrefix, чтобы получить структуру таблицы.
	Block getSampleBlock() const
	{
		return sample_block;
	}


	void writePrefix() override
	{
		/** Отправляет запрос и получает блок-пример, описывающий структуру таблицы.
		  * Он нужен, чтобы знать, какие блоки передавать в метод write.
		  */

		connection.sendQuery(query, "", QueryProcessingStage::Complete, settings);

		Connection::Packet packet = connection.receivePacket();

		if (Protocol::Server::Data == packet.type)
		{
			sample_block = packet.block;

			if (!sample_block)
				throw Exception("Logical error: empty block received as table structure", ErrorCodes::LOGICAL_ERROR);
		}
		else if (Protocol::Server::Exception == packet.type)
		{
			packet.exception->rethrow();
			return;
		}
		else
			throw NetException("Unexpected packet from server (expected Data or Exception, got "
				+ String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
	}


	void write(const Block & block) override
	{
		if (!sample_block)
			throw Exception("You must call IBlockOutputStream::writePrefix before IBlockOutputStream::write", ErrorCodes::LOGICAL_ERROR);

		if (!blocksHaveEqualStructure(block, sample_block))
		{
			std::stringstream message;
			message << "Block structure is different from table structure.\n"
				<< "\nTable structure:\n(" << sample_block.dumpStructure() << ")\nBlock structure:\n(" << block.dumpStructure() << ")\n";

			LOG_ERROR(&Logger::get("RemoteBlockOutputStream"), message.str());
			throw DB::Exception(message.str());
		}

		connection.sendData(block);
	}


	/// Отправить блок данных, который уже был заранее сериализован (и, если надо, сжат), который следует прочитать из input-а.
	void writePrepared(ReadBuffer & input, size_t size = 0)
	{
		/// Не можем использовать sample_block.
		connection.sendPreparedData(input, size);
	}


	void writeSuffix() override
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
			throw NetException("Unexpected packet from server (expected EndOfStream or Exception, got "
				+ String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
	}

private:
	Connection & connection;
	String query;
	const Settings * settings;
	Block sample_block;
};

}
