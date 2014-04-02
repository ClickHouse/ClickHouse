#pragma once

#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/AbandonableLockInZooKeeper.h>


namespace DB
{

class ReplicatedMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_, const String & insert_id_)
		: storage(storage_), insert_id(insert_id_), block_index(0) {}

	void write(const Block & block) override
	{
		auto part_blocks = storage.writer.splitBlockIntoParts(block);
		for (auto & current_block : part_blocks)
		{
			++block_index;
			String block_id = insert_id.empty() ? "" : insert_id + "__" + toString(block_index);

			AbandonableLockInZooKeeper block_number_lock(
				storage.zookeeper_path + "/block-numbers/block-",
				storage.zookeeper_path + "/temp", storage.zookeeper);

			UInt64 part_number = block_number_lock.getNumber();

			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, part_number);

			String expected_checksums_str;
			if (!block_id.empty() && storage.zookeeper.tryGet(
				storage.zookeeper_path + "/blocks/" + block_id + "/checksums", expected_checksums_str))
			{
				/// Блок с таким ID уже когда-то вставляли. Проверим чексуммы и не будем его вставлять.
				auto expected_checksums = MergeTreeData::DataPart::Checksums.parse(expected_checksums_str);
				expected_checksums.check(part->checksums);

				/// Бросаем block_number_lock.
				continue;
			}

			storage.data.renameTempPartAndAdd(part);

			zkutil::Ops ops;
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
				toString(part_numbre),
				storage.zookeeper.getDefaultACL(),
				zkutil::CreateMode::Persistent));
			ops.push_back(new zkutil::Op::Create(
				storage.replica_path + "/parts/" + block_id + "/number",
				toString(part_numbre),
				storage.zookeeper.getDefaultACL(),
				zkutil::CreateMode::Persistent));

           const std::vector<data::ACL>& acl, CreateMode::type mode);

			block_number_lock.unlock();
		}
	}

private:
	StorageReplicatedMergeTree & storage;
	String insert_id;
	size_t block_index;
};

}
