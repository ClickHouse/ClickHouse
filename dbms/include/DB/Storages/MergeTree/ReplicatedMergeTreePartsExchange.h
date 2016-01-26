#pragma once

#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/HashingWriteBuffer.h>
#include <DB/IO/copyData.h>


namespace DB
{

class StorageReplicatedMergeTree;

class ReplicatedMergeTreePartsServer : public InterserverIOEndpoint
{
public:
	ReplicatedMergeTreePartsServer(MergeTreeData & data_, StorageReplicatedMergeTree & storage_) : data(data_),
		storage(storage_), log(&Logger::get(data.getLogName() + " (Replicated PartsServer)")) {}

	void processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out) override;

private:
	MergeTreeData & data;
	StorageReplicatedMergeTree & storage;

	Logger * log;

	MergeTreeData::DataPartPtr findPart(const String & name)
	{
		MergeTreeData::DataPartPtr part = data.getPartIfExists(name);
		if (part)
			return part;
		throw Exception("No part " + name + " in table");
	}
};

class ReplicatedMergeTreePartsFetcher
{
public:
	ReplicatedMergeTreePartsFetcher(MergeTreeData & data_) : data(data_), log(&Logger::get("ReplicatedMergeTreePartsFetcher")) {}

	/// Скачивает кусок в tmp_директорию. Если to_detached - скачивает в директорию detached.
	MergeTreeData::MutableDataPartPtr fetchPart(
		const String & part_name,
		const String & replica_path,
		const String & host,
		int port,
		bool to_detached = false);

	void cancel() { is_cancelled = true; }

private:
	MergeTreeData & data;

	/// Нужно остановить передачу данных.
	std::atomic<bool> is_cancelled {false};

	Logger * log;
};

}
