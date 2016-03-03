#pragma once

#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/HashingWriteBuffer.h>
#include <DB/IO/copyData.h>


namespace DB
{

namespace DataPartsExchange
{

/** Сервис для отправки кусков из таблицы *MergeTree.
  */
class Service final : public InterserverIOEndpoint
{
public:
	Service(MergeTreeData & data_, StoragePtr & storage_) : data(data_),
		owned_storage(storage_), log(&Logger::get(data.getLogName() + " (Replicated PartsService)")) {}

	Service(const Service &) = delete;
	Service & operator=(const Service &) = delete;

	std::string getId(const std::string & node_id) const override;
	void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out) override;

private:
	MergeTreeData::DataPartPtr findPart(const String & name);
	MergeTreeData::DataPartPtr findShardedPart(const String & name, size_t shard_no);

private:
	MergeTreeData & data;
	StoragePtr owned_storage;
	Logger * log;
};

/** Клиент для получения кусков из таблицы *MergeTree.
  */
class Fetcher final
{
public:
	Fetcher(MergeTreeData & data_) : data(data_), log(&Logger::get("Fetcher")) {}

	Fetcher(const Fetcher &) = delete;
	Fetcher & operator=(const Fetcher &) = delete;

	/// Скачивает кусок в tmp_директорию. Если to_detached - скачивает в директорию detached.
	MergeTreeData::MutableDataPartPtr fetchPart(
		const String & part_name,
		const String & replica_path,
		const String & host,
		int port,
		bool to_detached = false);

	/// Метод для перешардирования. Скачивает шардированный кусок
	/// из заданного шарда в папку to_detached.
	MergeTreeData::MutableDataPartPtr fetchShardedPart(
		const InterserverIOEndpointLocation & location,
		const String & part_name,
		size_t shard_no);

	void cancel() { is_cancelled = true; }

private:
	MergeTreeData::MutableDataPartPtr fetchPartImpl(
		const String & part_name,
		const String & replica_path,
		const String & host,
		int port,
		const String & shard_no,
		bool to_detached);

private:
	MergeTreeData & data;
	/// Нужно остановить передачу данных.
	std::atomic<bool> is_cancelled {false};
	Logger * log;
};

}

}
