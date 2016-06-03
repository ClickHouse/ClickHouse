#pragma once

#include <DB/Core/Types.h>
#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/IO/WriteBuffer.h>

namespace DB
{

class Context;

namespace RemoteDiskSpaceMonitor
{

/** Сервис для получения информации о свободном месте на диске.
  */
class Service final : public InterserverIOEndpoint
{
public:
	Service(const Context & context_);
	Service(const Service &) = delete;
	Service & operator=(const Service &) = delete;
	std::string getId(const std::string & node_id) const override;
	void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out) override;

private:
	const Context & context;
};

/** Клиент для получения информации о свободном месте на удалённом диске.
  */
class Client final
{
public:
	Client() = default;
	Client(const Client &) = delete;
	Client & operator=(const Client &) = delete;
	size_t getFreeSpace(const InterserverIOEndpointLocation & location) const;
	void cancel() { is_cancelled = true; }

private:
	std::atomic<bool> is_cancelled{false};
};

}

}
