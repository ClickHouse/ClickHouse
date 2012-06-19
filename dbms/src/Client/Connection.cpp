#include <Poco/Net/NetException.h>

#include <Yandex/Revision.h>

#include <DB/Core/Defines.h>

#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>

#include <DB/Client/Connection.h>


namespace DB
{

void Connection::connect()
{
	socket.connect(Poco::Net::SocketAddress(host, port));
	connected = true;

	sendHello();
	receiveHello();
}


void Connection::sendHello()
{
	writeVarUInt(Protocol::Client::Hello, *out);
	writeStringBinary((DBMS_NAME " ") + client_name, *out);
	writeVarUInt(DBMS_VERSION_MAJOR, *out);
	writeVarUInt(DBMS_VERSION_MINOR, *out);
	writeVarUInt(Revision::get(), *out);
	writeStringBinary(default_database, *out);
	
	out->next();
}


void Connection::receiveHello()
{
	/// Получить hello пакет.
	UInt64 packet_type = 0;

	readVarUInt(packet_type, *in);
	if (packet_type == Protocol::Server::Hello)
	{
		readStringBinary(server_name, *in);
		readVarUInt(server_version_major, *in);
		readVarUInt(server_version_minor, *in);
		readVarUInt(server_revision, *in);
	}
	else if (packet_type == Protocol::Server::Exception)
		receiveException()->rethrow();
	else
		throw Exception("Unexpected packet from server (expected Hello or Exception, got "
			+ String(Protocol::Server::toString(Protocol::Server::Enum(packet_type))) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}


void Connection::getServerVersion(String & name, UInt64 & version_major, UInt64 & version_minor, UInt64 & revision)
{
	if (!connected)
		connect();
	
	name = server_name;
	version_major = server_version_major;
	version_minor = server_version_minor;
	revision = server_revision;
}


void Connection::forceConnected()
{
	if (!connected)
		connect();
	else
	{
		try
		{
			if (!ping())
			{
				socket.close();
				connect();
			}
		}
		catch (const Poco::Net::NetException & e)
		{
			connect();
		}
	}
}


bool Connection::ping()
{
	UInt64 pong = 0;
	writeVarUInt(Protocol::Client::Ping, *out);
	out->next();

	if (in->eof())
		return false;

	readVarUInt(pong, *in);

	/// Можем получить запоздалые пакеты прогресса.
	while (pong == Protocol::Server::Progress)
	{
		receiveProgress();
		readVarUInt(pong, *in);
	}

	if (pong != Protocol::Server::Pong)
		throw Exception("Unexpected packet from server (expected Pong, got "
			+ String(Protocol::Server::toString(Protocol::Server::Enum(pong))) + ")",
			ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);

	return true;
}


void Connection::sendQuery(const String & query, UInt64 query_id_, UInt64 stage)
{
	forceConnected();
	
	query_id = query_id_;

	writeVarUInt(Protocol::Client::Query, *out);
	writeIntBinary(query_id, *out);
	writeVarUInt(stage, *out);
	writeVarUInt(compression, *out);

	writeStringBinary(query, *out);

	out->next();

	maybe_compressed_in = NULL;
	maybe_compressed_out = NULL;
	block_in = NULL;
	block_out = NULL;
}


void Connection::sendCancel()
{
	writeVarUInt(Protocol::Client::Cancel, *out);
	out->next();
}


void Connection::sendData(const Block & block)
{
	if (!block_out)
	{
		if (compression == Protocol::Compression::Enable)
			maybe_compressed_out = new CompressedWriteBuffer(*out);
		else
			maybe_compressed_out = out;

		block_out = new NativeBlockOutputStream(*maybe_compressed_out);
	}

	writeVarUInt(Protocol::Client::Data, *out);
	block_out->write(block);
	maybe_compressed_out->next();
	out->next();
}


bool Connection::poll(size_t timeout_microseconds)
{
	return in->poll(timeout_microseconds);
}


Connection::Packet Connection::receivePacket()
{
	Packet res;
	readVarUInt(res.type, *in);

	switch (res.type)
	{
		case Protocol::Server::Data:
			res.block = receiveData();
			return res;

		case Protocol::Server::Exception:
			res.exception = receiveException();
			return res;

		case Protocol::Server::Progress:
			res.progress = receiveProgress();
			return res;

		case Protocol::Server::EndOfStream:
			return res;

		default:
			throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
	}
}


Block Connection::receiveData()
{
	if (!block_in)
	{
		if (compression == Protocol::Compression::Enable)
			maybe_compressed_in = new CompressedReadBuffer(*in);
		else
			maybe_compressed_in = in;

		block_in = new NativeBlockInputStream(*maybe_compressed_in, data_type_factory);
	}

	/// Прочитать из сети один блок
	return block_in->read();
}


SharedPtr<Exception> Connection::receiveException()
{
	Exception e;
	readException(e, *in);
	return e.clone();
}


Progress Connection::receiveProgress()
{
	Progress progress;
	progress.read(*in);
	return progress;
}

}
