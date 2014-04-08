#pragma once

#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/AbandonableLockInZooKeeper.h>


namespace DB
{

class ReplicatedMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_, const String & insert_id_)
		: storage(storage_), insert_id(insert_id_), block_index(0), log(&Logger::get("ReplicatedMergeTreeBlockOutputStream")) {}

	void write(const Block & block) override
	{
		auto part_blocks = storage.writer.splitBlockIntoParts(block);
		for (auto & current_block : part_blocks)
		{
			++block_index;
			String block_id = insert_id.empty() ? "" : insert_id + "__" + toString(block_index);

			AbandonableLockInZooKeeper block_number_lock(
				storage.zookeeper_path + "/block_numbers/block-",
				storage.zookeeper_path + "/temp", storage.zookeeper);

			UInt64 part_number = block_number_lock.getNumber();

			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, part_number);

			String expected_checksums_str;
			if (!block_id.empty() && storage.zookeeper.tryGet(
				storage.zookeeper_path + "/blocks/" + block_id + "/checksums", expected_checksums_str))
			{
				LOG_INFO(log, "Block with this ID already exists; ignoring it");

				/// Блок с таким ID уже когда-то вставляли. Проверим чексуммы и не будем его вставлять.
				auto expected_checksums = MergeTreeData::DataPart::Checksums::parse(expected_checksums_str);
				expected_checksums.check(part->checksums);

				part->remove();

				/// Бросаем block_number_lock.
				continue;
			}

			storage.data.renameTempPartAndAdd(part);

			StorageReplicatedMergeTree::LogEntry log_entry;
			log_entry.type = StorageReplicatedMergeTree::LogEntry::GET_PART;
			log_entry.source_replica = storage.replica_name;
			log_entry.new_part_name = part->name;

			/// Одновременно добавим информацию о куске во все нужные места в ZooKeeper и снимем block_number_lock.
			zkutil::Ops ops;
			if (!block_id.empty())
			{
				ops.push_back(new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id,
					"",
					storage.zookeeper.getDefaultACL(),
					zkutil::CreateMode::Persistent));
				ops.push_back(new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id + "/checksums",
					part->checksums.toString(),
					storage.zookeeper.getDefaultACL(),
					zkutil::CreateMode::Persistent));
				ops.push_back(new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id + "/number",
					toString(part_number),
					storage.zookeeper.getDefaultACL(),
					zkutil::CreateMode::Persistent));
			}
			storage.checkPartAndAddToZooKeeper(part, ops);
			ops.push_back(new zkutil::Op::Create(
				storage.replica_path + "/log/log-",
				log_entry.toString(),
				storage.zookeeper.getDefaultACL(),
				zkutil::CreateMode::PersistentSequential));
			block_number_lock.getUnlockOps(ops);

			storage.zookeeper.multi(ops);
		}
	}

private:
	StorageReplicatedMergeTree & storage;
	String insert_id;
	size_t block_index;

	Logger * log;
};

}
