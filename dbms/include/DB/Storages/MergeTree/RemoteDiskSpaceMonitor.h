#pragma once

#include <DB/Core/Types.h>
#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/IO/WriteBuffer.h>

namespace DB
{

namespace RemoteDiskSpaceMonitor
{

/** Сервис для получения информации о свободном месте на диске.
  */
class Service final : public InterserverIOEndpoint
{
public:
	Service(const std::string & path_);
	Service(const Service &) = delete;
	Service & operator=(const Service &) = delete;
	std::string getId(const std::string & node_id) const override;
	void processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out) override;

private:
	const std::string path;
};

/** Клиент для получения информации о свободном месте на удалённом диске.
  */
class Client final
{
public:
	Client() = default;
	Client(const Client &) = delete;
	Client & operator=(const Client &) = delete;
	size_t getFreeDiskSpace(const InterserverIOEndpointLocation & location) const;
	void cancel() { is_cancelled = true; }

private:
	std::atomic<bool> is_cancelled{false};
};

}

}
