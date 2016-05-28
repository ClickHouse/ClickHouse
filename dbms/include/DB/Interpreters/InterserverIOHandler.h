#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Core/Types.h>
#include <map>
#include <atomic>
#include <Poco/Net/HTMLForm.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int DUPLICATE_INTERSERVER_IO_ENDPOINT;
	extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT;
}

/** Местонахождение сервиса.
  */
struct InterserverIOEndpointLocation
{
public:
	InterserverIOEndpointLocation(const std::string & name_, const std::string & host_, UInt16 port_)
		: name(name_), host(host_), port(port_)
	{
	}

	/// Создаёт местонахождение на основе его сериализованного представления.
	InterserverIOEndpointLocation(const std::string & serialized_location)
	{
		ReadBufferFromString buf(serialized_location);
		readBinary(name, buf);
		readBinary(host, buf);
		readBinary(port, buf);
		assertEOF(buf);
	}

	/// Сериализует местонахождение.
	std::string toString() const
	{
		std::string serialized_location;
		WriteBufferFromString buf(serialized_location);
		writeBinary(name, buf);
		writeBinary(host, buf);
		writeBinary(port, buf);
		buf.next();
		return serialized_location;
	}

public:
	std::string name;
	std::string host;
	UInt16 port;
};

/** Обработчик запросов от других серверов.
  */
class InterserverIOEndpoint
{
public:
	virtual std::string getId(const std::string & path) const = 0;
	virtual void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out) = 0;
	virtual ~InterserverIOEndpoint() {}

	void cancel() { is_cancelled = true; }

protected:
	/// Нужно остановить передачу данных.
	std::atomic<bool> is_cancelled {false};
};

using InterserverIOEndpointPtr = std::shared_ptr<InterserverIOEndpoint>;


/** Сюда можно зарегистрировать сервис, обрататывающий запросы от других серверов.
  * Используется для передачи кусков в ReplicatedMergeTree.
  */
class InterserverIOHandler
{
public:
	void addEndpoint(const String & name, InterserverIOEndpointPtr endpoint)
	{
		std::lock_guard<std::mutex> lock(mutex);
		if (endpoint_map.count(name))
			throw Exception("Duplicate interserver IO endpoint: " + name, ErrorCodes::DUPLICATE_INTERSERVER_IO_ENDPOINT);
		endpoint_map[name] = endpoint;
	}

	void removeEndpoint(const String & name)
	{
		std::lock_guard<std::mutex> lock(mutex);
		if (!endpoint_map.count(name))
			throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
		endpoint_map.erase(name);
	}

	InterserverIOEndpointPtr getEndpoint(const String & name)
	{
		std::lock_guard<std::mutex> lock(mutex);
		if (!endpoint_map.count(name))
			throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
		return endpoint_map[name];
	}

private:
	using EndpointMap = std::map<String, InterserverIOEndpointPtr>;

	EndpointMap endpoint_map;
	std::mutex mutex;
};

/// В конструкторе вызывает addEndpoint, в деструкторе - removeEndpoint.
class InterserverIOEndpointHolder
{
public:
	InterserverIOEndpointHolder(const String & name_, InterserverIOEndpointPtr endpoint_, InterserverIOHandler & handler_)
		: name(name_), endpoint(endpoint_), handler(handler_)
	{
		handler.addEndpoint(name, endpoint);
	}

	InterserverIOEndpointPtr getEndpoint()
	{
		return endpoint;
	}

	~InterserverIOEndpointHolder()
	{
		try
		{
			handler.removeEndpoint(name);
			/// После уничтожения объекта, endpoint ещё может жить, так как владение им захватывается на время обработки запроса,
			/// см. InterserverIOHTTPHandler.cpp
		}
		catch (...)
		{
			tryLogCurrentException("~InterserverIOEndpointHolder");
		}
	}

	void cancel() { endpoint->cancel(); }

private:
	String name;
	InterserverIOEndpointPtr endpoint;
	InterserverIOHandler & handler;
};

using InterserverIOEndpointHolderPtr = std::shared_ptr<InterserverIOEndpointHolder>;

}
