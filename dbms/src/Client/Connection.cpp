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
	try
	{
		if (connected)
			disconnect();
		
		LOG_TRACE(log, "Connecting");

		socket.connect(Poco::Net::SocketAddress(host, port), connect_timeout);
		socket.setReceiveTimeout(receive_timeout);
		socket.setSendTimeout(send_timeout);

		in = new ReadBufferFromPocoSocket(socket);
		out = new WriteBufferFromPocoSocket(socket);

		connected = true;

		sendHello();
		receiveHello();

		LOG_TRACE(log, "Connected to " << server_name
			<< " server version " << server_version_major
			<< "." << server_version_minor
			<< "." << server_revision
			<< ".");
	}
	catch (Poco::Net::NetException & e)
	{
		disconnect();

		/// Добавляем в сообщение адрес сервера. Жаль, что более точный тип исключения теряется.
		throw Poco::Net::NetException(e.displayText(), "(" + getServerAddress() + ")", e.code());
	}
	catch (Poco::TimeoutException & e)
	{
		disconnect();

		/// Добавляем в сообщение адрес сервера. Жаль, что более точный тип исключения теряется.
		throw Poco::TimeoutException(e.displayText(), "(" + getServerAddress() + ")", e.code());
	}
}


void Connection::disconnect()
{
	//LOG_TRACE(log, "Disconnecting (" << getServerAddress() << ")");
	
	socket.close();
	in = NULL;
	out = NULL;
	connected = false;
}


void Connection::sendHello()
{
	//LOG_TRACE(log, "Sending hello (" << getServerAddress() << ")");

	writeVarUInt(Protocol::Client::Hello, *out);
	writeStringBinary((DBMS_NAME " ") + client_name, *out);
	writeVarUInt(DBMS_VERSION_MAJOR, *out);
	writeVarUInt(DBMS_VERSION_MINOR, *out);
	writeVarUInt(Revision::get(), *out);
	writeStringBinary(default_database, *out);
	writeStringBinary(user, *out);
	writeStringBinary(password, *out);
	
	out->next();
}


void Connection::receiveHello()
{
	//LOG_TRACE(log, "Receiving hello (" << getServerAddress() << ")");
	
	/// Получить hello пакет.
	UInt64 packet_type = 0;

	readVarUInt(packet_type, *in);
	if (packet_type == Protocol::Server::Hello)
	{
		readStringBinary(server_name, *in);
		readVarUInt(server_version_major, *in);
		readVarUInt(server_version_minor, *in);
		readVarUInt(server_revision, *in);

		/// Старые ревизии сервера не поддерживают имя пользователя и пароль, которые были отправлены в пакете hello.
		if (server_revision < DBMS_MIN_REVISION_WITH_USER_PASSWORD)
			throw Exception("Server revision is too old for this client. You must update server to at least " + toString(DBMS_MIN_REVISION_WITH_USER_PASSWORD) + ".",
				ErrorCodes::SERVER_REVISION_IS_TOO_OLD);
	}
	else if (packet_type == Protocol::Server::Exception)
		receiveException()->rethrow();
	else
	{
		/// Закроем соединение, чтобы не было рассинхронизации.
		disconnect();

		throw Exception("Unexpected packet from server " + getServerAddress() + " (expected Hello or Exception, got "
			+ String(Protocol::Server::toString(packet_type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
	}
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
	{
		connect();
	}
	else if (!ping())
	{
		LOG_TRACE(log, "Connection was closed, will reconnect.");
		connect();
	}
}


bool Connection::ping()
{
	//LOG_TRACE(log, "Ping (" << getServerAddress() << ")");
	
	try
	{
		UInt64 pong = 0;
		writeVarUInt(Protocol::Client::Ping, *out);
		out->next();

		if (in->eof())
			return false;

		readVarUInt(pong, *in);

		/// Можем получить запоздалые пакеты прогресса. TODO: может быть, это можно исправить.
		while (pong == Protocol::Server::Progress)
		{
			receiveProgress();

			if (in->eof())
				return false;

			readVarUInt(pong, *in);
		}

		if (pong != Protocol::Server::Pong)
		{
			throw Exception("Unexpected packet from server " + getServerAddress() + " (expected Pong, got "
				+ String(Protocol::Server::toString(pong)) + ")",
				ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
		}
	}
	catch (const Poco::Exception & e)
	{
		LOG_TRACE(log, e.displayText());
		return false;
	}

	return true;
}


void Connection::sendQuery(const String & query, UInt64 query_id_, UInt64 stage, const Settings * settings)
{
	forceConnected();
	
	query_id = query_id_;

	//LOG_TRACE(log, "Sending query (" << getServerAddress() << ")");

	writeVarUInt(Protocol::Client::Query, *out);
	writeIntBinary(query_id, *out);

	/// Настройки на отдельный запрос.
	if (server_revision >= DBMS_MIN_REVISION_WITH_PER_QUERY_SETTINGS)
	{
		if (settings)
			settings->serialize(*out);
		else
			writeStringBinary("", *out);
	}
	
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
	//LOG_TRACE(log, "Sending cancel (" << getServerAddress() << ")");
	
	writeVarUInt(Protocol::Client::Cancel, *out);
	out->next();
}


void Connection::sendData(const Block & block)
{
	//LOG_TRACE(log, "Sending data (" << getServerAddress() << ")");
	
	if (!block_out)
	{
		if (compression == Protocol::Compression::Enable)
			maybe_compressed_out = new CompressedWriteBuffer(*out);
		else
			maybe_compressed_out = out;

		block_out = new NativeBlockOutputStream(*maybe_compressed_out);
	}

	writeVarUInt(Protocol::Client::Data, *out);
	block.checkNestedArraysOffsets();
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
	//LOG_TRACE(log, "Receiving packet (" << getServerAddress() << ")");
	
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
			
		case Protocol::Server::ProfileInfo:
			res.profile_info = receiveProfileInfo();
			return res;

		case Protocol::Server::EndOfStream:
			return res;

		default:
			/// Закроем соединение, чтобы не было рассинхронизации.
			disconnect();
			throw Exception("Unknown packet "
				+ toString(res.type)
				+ " from server " + getServerAddress(), ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
	}
}


Block Connection::receiveData()
{
	//LOG_TRACE(log, "Receiving data (" << getServerAddress() << ")");
		
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


String Connection::getServerAddress() const
{
	return Poco::Net::SocketAddress(host, port).toString();
}


SharedPtr<Exception> Connection::receiveException()
{
	//LOG_TRACE(log, "Receiving exception (" << getServerAddress() << ")");
	
	Exception e;
	readException(e, *in, "Received from " + getServerAddress());
	return e.clone();
}


Progress Connection::receiveProgress()
{
	//LOG_TRACE(log, "Receiving progress (" << getServerAddress() << ")");
	
	Progress progress;
	progress.read(*in);
	return progress;
}


BlockStreamProfileInfo Connection::receiveProfileInfo()
{
	BlockStreamProfileInfo profile_info;
	profile_info.read(*in);
	return profile_info;
}

}
