#pragma once

#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/AbandonableLockInZooKeeper.h>
#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

class ReplicatedMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_, const String & insert_id_)
		: storage(storage_), insert_id(insert_id_),
		log(&Logger::get(storage.data.getLogName() + " (Replicated OutputStream)")) {}

	void writePrefix() override
	{
		/// TODO Можно ли здесь не блокировать структуру таблицы?
		storage.data.delayInsertIfNeeded(&storage.restarting_thread->getWakeupEvent());
	}

	void write(const Block & block) override
	{
		auto zookeeper = storage.getZooKeeper();

		assertSessionIsNotExpired(zookeeper);
		auto part_blocks = storage.writer.splitBlockIntoParts(block);

		for (auto & current_block : part_blocks)
		{
			assertSessionIsNotExpired(zookeeper);

			++block_index;
			String block_id = insert_id.empty() ? "" : insert_id + "__" + toString(block_index);
			String month_name = toString(DateLUT::instance().toNumYYYYMMDD(DayNum_t(current_block.min_date)) / 100);

			AbandonableLockInZooKeeper block_number_lock = storage.allocateBlockNumber(month_name);

			Int64 part_number = block_number_lock.getNumber();

			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, part_number);
			String part_name = ActiveDataPartSet::getPartName(part->left_date, part->right_date, part->left, part->right, part->level);

			/// Если в запросе не указан ID, возьмем в качестве ID хеш от данных. То есть, не вставляем одинаковые данные дважды.
			/// NOTE: Если такая дедупликация не нужна, можно вместо этого оставлять block_id пустым.
			///       Можно для этого сделать настройку или синтаксис в запросе (например, ID=null).
			if (block_id.empty())
				block_id = part->checksums.summaryDataChecksum();

			LOG_DEBUG(log, "Wrote block " << part_number << " with ID " << block_id << ", " << current_block.block.rows() << " rows");

			StorageReplicatedMergeTree::LogEntry log_entry;
			log_entry.type = StorageReplicatedMergeTree::LogEntry::GET_PART;
			log_entry.source_replica = storage.replica_name;
			log_entry.new_part_name = part_name;

			/// Одновременно добавим информацию о куске во все нужные места в ZooKeeper и снимем block_number_lock.
			zkutil::Ops ops;
			if (!block_id.empty())
			{
				ops.push_back(new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id,
					"",
					zookeeper->getDefaultACL(),
					zkutil::CreateMode::Persistent));
				ops.push_back(new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id + "/columns",
					part->columns.toString(),
					zookeeper->getDefaultACL(),
					zkutil::CreateMode::Persistent));
				ops.push_back(new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id + "/checksums",
					part->checksums.toString(),
					zookeeper->getDefaultACL(),
					zkutil::CreateMode::Persistent));
				ops.push_back(new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id + "/number",
					toString(part_number),
					zookeeper->getDefaultACL(),
					zkutil::CreateMode::Persistent));
			}
			storage.checkPartAndAddToZooKeeper(part, ops, part_name);
			ops.push_back(new zkutil::Op::Create(
				storage.zookeeper_path + "/log/log-",
				log_entry.toString(),
				zookeeper->getDefaultACL(),
				zkutil::CreateMode::PersistentSequential));
			block_number_lock.getUnlockOps(ops);

			MergeTreeData::Transaction transaction; /// Если не получится добавить кусок в ZK, снова уберем его из рабочего набора.
			storage.data.renameTempPartAndAdd(part, nullptr, &transaction);

			try
			{
				auto code = zookeeper->tryMulti(ops);
				if (code == ZOK)
				{
					transaction.commit();
					storage.merge_selecting_event.set();
				}
				else if (code == ZNODEEXISTS)
				{
					/// Если блок с таким ID уже есть в таблице, откатим его вставку.
					String expected_checksums_str;
					if (!block_id.empty() && zookeeper->tryGet(
						storage.zookeeper_path + "/blocks/" + block_id + "/checksums", expected_checksums_str))
					{
						LOG_INFO(log, "Block with ID " << block_id << " already exists; ignoring it (removing part " << part->name << ")");

						auto expected_checksums = MergeTreeData::DataPart::Checksums::parse(expected_checksums_str);

						/// Если данные отличались от тех, что были вставлены ранее с тем же ID, бросим исключение.
						expected_checksums.checkEqual(part->checksums, true);
					}
					else
					{
						throw Exception("Unexpected ZNODEEXISTS while adding block " + toString(part_number) + " with ID " + block_id + ": "
							+ zkutil::ZooKeeper::error2string(code), ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR);
					}
				}
				else
				{
					throw Exception("Unexpected error while adding block " + toString(part_number) + " with ID " + block_id + ": "
						+ zkutil::ZooKeeper::error2string(code), ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR);
				}
			}
			catch (const zkutil::KeeperException & e)
			{
				/** Если потерялось соединение, и мы не знаем, применились ли изменения, нельзя удалять локальный кусок:
				  *  если изменения применились, в /blocks/ появился вставленный блок, и его нельзя будет вставить снова.
				  */
				if (e.code == ZOPERATIONTIMEOUT ||
					e.code == ZCONNECTIONLOSS)
				{
					transaction.commit();
					storage.enqueuePartForCheck(part->name);
				}

				throw;
			}
		}
	}

private:
	StorageReplicatedMergeTree & storage;
	String insert_id;
	size_t block_index = 0;

	Logger * log;


	/// Позволяет проверить, что сессия в ZooKeeper ещё жива.
	void assertSessionIsNotExpired(zkutil::ZooKeeperPtr & zookeeper)
	{
		if (zookeeper->expired())
			throw Exception("ZooKeeper session has been expired.", ErrorCodes::NO_ZOOKEEPER);
	}
};

}
