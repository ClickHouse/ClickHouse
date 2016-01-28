#pragma once

#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/IO/WriteBuffer.h>
#include <common/logger_useful.h>

namespace DB
{

class StorageReplicatedMergeTree;

namespace ShardedPartitionSender
{

/** Сервис для получения кусков из партиции таблицы *MergeTree.
  */
class Service final : public InterserverIOEndpoint
{
public:
	Service(StorageReplicatedMergeTree & storage_);
	Service(const Service &) = delete;
	Service & operator=(const Service &) = delete;
	std::string getId(const std::string & node_id) const override;
	void processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out) override;

private:
	StorageReplicatedMergeTree & storage;
	Logger * log;
};

/** Клиент для отправления кусков из партиции таблицы *MergeTree.
  */
class Client final
{
public:
	Client();
	Client(const Client &) = delete;
	Client & operator=(const Client &) = delete;
	bool send(const InterserverIOEndpointLocation & to_location, const InterserverIOEndpointLocation & from_location,
		const std::vector<std::string> & parts, size_t shard_no);
	void cancel() { is_cancelled = true; }

private:
	std::atomic<bool> is_cancelled{false};
	Logger * log;
};

}

}
