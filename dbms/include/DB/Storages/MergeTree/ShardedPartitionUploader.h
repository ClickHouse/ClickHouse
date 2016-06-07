#pragma once

#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/IO/WriteBuffer.h>
#include <common/logger_useful.h>
#include <functional>

namespace DB
{

class StorageReplicatedMergeTree;

namespace ShardedPartitionUploader
{

/** Сервис для получения кусков из партиции таблицы *MergeTree.
  */
class Service final : public InterserverIOEndpoint
{
public:
	Service(StoragePtr & storage_);
	Service(const Service &) = delete;
	Service & operator=(const Service &) = delete;
	std::string getId(const std::string & node_id) const override;
	void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out) override;

private:
	StoragePtr owned_storage;
	MergeTreeData & data;
	Logger * log = &Logger::get("ShardedPartitionUploader::Service");
};

/** Клиент для отправления кусков из партиции таблицы *MergeTree.
  */
class Client final
{
public:
	using CancellationHook = std::function<void()>;

public:
	Client(StorageReplicatedMergeTree & storage_);

	Client(const Client &) = delete;
	Client & operator=(const Client &) = delete;

	void setCancellationHook(CancellationHook cancellation_hook_);

	bool send(const std::string & part_name, size_t shard_no,
		const InterserverIOEndpointLocation & to_location);

	void cancel() { is_cancelled = true; }

private:
	MergeTreeData::DataPartPtr findShardedPart(const std::string & name, size_t shard_no);
	void abortIfRequested();

private:
	StorageReplicatedMergeTree & storage;
	MergeTreeData & data;
	CancellationHook cancellation_hook;
	std::atomic<bool> is_cancelled{false};
	Logger * log = &Logger::get("ShardedPartitionUploader::Client");
};

}

}
