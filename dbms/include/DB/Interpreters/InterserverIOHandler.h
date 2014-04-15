#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/Core/Types.h>
#include <map>
#include <Poco/Net/HTMLForm.h>

namespace DB
{

/** Обработчик запросов от других серверов.
  */
class InterserverIOEndpoint
{
public:
	virtual void processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out) = 0;

	virtual ~InterserverIOEndpoint() {}
};

typedef Poco::SharedPtr<InterserverIOEndpoint> InterserverIOEndpointPtr;


/** Сюда можно зарегистрировать сервис, обрататывающий запросы от других серверов.
  * Используется для передачи кусков в ReplicatedMergeTree.
  */
class InterserverIOHandler
{
public:
	void addEndpoint(const String & name, InterserverIOEndpointPtr endpoint)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		if (endpoint_map.count(name))
			throw Exception("Duplicate interserver IO endpoint: " + name, ErrorCodes::DUPLICATE_INTERSERVER_IO_ENDPOINT);
		endpoint_map[name] = endpoint;
	}

	void removeEndpoint(const String & name)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		if (!endpoint_map.count(name))
			throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
		endpoint_map.erase(name);
	}

	InterserverIOEndpointPtr getEndpoint(const String & name)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		if (!endpoint_map.count(name))
			throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
		return endpoint_map[name];
	}

private:
	typedef std::map<String, InterserverIOEndpointPtr> EndpointMap;

	EndpointMap endpoint_map;
	Poco::FastMutex mutex;
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
		}
		catch (...)
		{
			tryLogCurrentException("~InterserverIOEndpointHolder");
		}
	}

private:
	String name;
	InterserverIOEndpointPtr endpoint;
	InterserverIOHandler & handler;
};

typedef Poco::SharedPtr<InterserverIOEndpointHolder> InterserverIOEndpointHolderPtr;

}
