#pragma once

#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/AbandonableLockInZooKeeper.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/IO/Operators.h>


namespace DB
{

class ReplicatedMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_, const String & insert_id_, size_t quorum_)
		: storage(storage_), insert_id(insert_id_), quorum(quorum_),
		log(&Logger::get(storage.data.getLogName() + " (Replicated OutputStream)"))
	{
		/// Значение кворума 1 имеет такой же смысл, как если он отключён.
		if (quorum == 1)
			quorum = 0;
	}

	void writePrefix() override
	{
		/// TODO Можно ли здесь не блокировать структуру таблицы?
		storage.data.delayInsertIfNeeded(&storage.restarting_thread->getWakeupEvent());
	}

	void write(const Block & block) override
	{
		auto zookeeper = storage.getZooKeeper();

		assertSessionIsNotExpired(zookeeper);

		/** Если запись с кворумом, то проверим, что требуемое количество реплик сейчас живо,
		  *  а также что для всех предыдущих кусков, для которых требуется кворум, этот кворум достигнут.
		  */
		String quorum_status_path = storage.zookeeper_path + "/quorum/status";
		if (quorum)
		{
			/// Список живых реплик. Все они регистрируют эфемерную ноду для leader_election.
			auto live_replicas = zookeeper->getChildren(storage.zookeeper_path + "/leader_election");

			if (live_replicas.size() < quorum)
				throw Exception("Number of alive replicas ("
					+ toString(live_replicas.size()) + ") is less than requested quorum (" + toString(quorum) + ").",
					ErrorCodes::TOO_LESS_LIVE_REPLICAS);

			/** Достигнут ли кворум для последнего куска, для которого нужен кворум?
			  * Запись всех кусков с включенным кворумом линейно упорядочена.
			  * Это значит, что в любой момент времени может быть только один кусок,
			  *  для которого нужен, но ещё не достигнут кворум.
			  * Информация о таком куске будет расположена в ноде /quorum/status.
			  * Если кворум достигнут, то нода удаляется.
			  */

			String quorum_status;
			bool quorum_unsatisfied = zookeeper->tryGet(quorum_status_path, quorum_status);

			if (quorum_unsatisfied)
				throw Exception("Quorum for previous write has not been satisfied yet. Status: " + quorum_status, ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE);

			/// Обе проверки неявно делаются и позже (иначе был бы race condition).
		}

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

			/// Хэш от данных.
			SipHash hash;
			part->checksums.summaryDataChecksum(hash);
			union
			{
				char bytes[16];
				UInt64 lo, hi;
			} hash_value;
			hash.get128(hash_value.bytes);

			String checksum(hash_value.bytes, 16);

			/// Если в запросе не указан ID, возьмем в качестве ID хеш от данных. То есть, не вставляем одинаковые данные дважды.
			/// NOTE: Если такая дедупликация не нужна, можно вместо этого оставлять block_id пустым.
			///       Можно для этого сделать настройку или синтаксис в запросе (например, ID=null).
			if (block_id.empty())
			{
				block_id = toString(hash_value.lo) + "_" + toString(hash_value.hi);

				if (block_id.empty())
					throw Exception("Logical error: block_id is empty.", ErrorCodes::LOGICAL_ERROR);
			}

			LOG_DEBUG(log, "Wrote block " << part_number << " with ID " << block_id << ", " << current_block.block.rows() << " rows");

			StorageReplicatedMergeTree::LogEntry log_entry;
			log_entry.type = StorageReplicatedMergeTree::LogEntry::GET_PART;
			log_entry.create_time = time(0);
			log_entry.source_replica = storage.replica_name;
			log_entry.new_part_name = part_name;
			log_entry.quorum = quorum;
			log_entry.block_id = block_id;

			/// Одновременно добавим информацию о куске во все нужные места в ZooKeeper и снимем block_number_lock.

			/// Информация о блоке.
			zkutil::Ops ops;
			auto acl = zookeeper->getDefaultACL();

			ops.push_back(
				new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id,
					"",
					acl,
					zkutil::CreateMode::Persistent));
			ops.push_back(
				new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id + "/checksum",
					checksum,
					acl,
					zkutil::CreateMode::Persistent));
			ops.push_back(
				new zkutil::Op::Create(
					storage.zookeeper_path + "/blocks/" + block_id + "/number",
					toString(part_number),
					acl,
					zkutil::CreateMode::Persistent));

			/// Информация о куске, в данных реплики.
			storage.checkPartAndAddToZooKeeper(part, ops, part_name);

			/// Лог репликации.
			ops.push_back(new zkutil::Op::Create(
				storage.zookeeper_path + "/log/log-",
				log_entry.toString(),
				acl,
				zkutil::CreateMode::PersistentSequential));

			/// Удаление информации о том, что номер блока используется для записи.
			block_number_lock.getUnlockOps(ops);

			/** Если нужен кворум - создание узла, в котором отслеживается кворум.
			  * (Если такой узел уже существует - значит кто-то успел одновременно сделать другую кворумную запись, но для неё кворум ещё не достигнут.
			  *  Делать в это время следующую кворумную запись нельзя.)
			  */
			if (quorum)
			{
				ReplicatedMergeTreeQuorumEntry quorum_entry;
				quorum_entry.part_name = part_name;
				quorum_entry.required_number_of_replicas = quorum;
				quorum_entry.replicas.insert(storage.replica_name);

				/** В данный момент, этот узел будет содержать информацию о том, что текущая реплика получила кусок.
				  * Когда другие реплики будут получать этот кусок (обычным способом, обрабатывая лог репликации),
				  *  они будут добавлять себя в содержимое этого узла.
				  * Когда в нём будет информация о quorum количестве реплик, этот узел удаляется,
				  *  что говорит о том, что кворум достигнут.
				  */

				ops.push_back(
					new zkutil::Op::Create(
						quorum_status_path,
						quorum_entry.toString(),
						acl,
						zkutil::CreateMode::Persistent));
			}

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
					String expected_checksum;
					if (!block_id.empty() && zookeeper->tryGet(
						storage.zookeeper_path + "/blocks/" + block_id + "/checksum", expected_checksum))
					{
						LOG_INFO(log, "Block with ID " << block_id << " already exists; ignoring it (removing part " << part->name << ")");

						/// Если данные отличались от тех, что были вставлены ранее с тем же ID, бросим исключение.
						if (expected_checksum != checksum)
						{
							if (!insert_id.empty())
								throw Exception("Attempt to insert block with same ID but different checksum", ErrorCodes::CHECKSUM_DOESNT_MATCH);
							else
								throw Exception("Logical error: got ZNODEEXISTS while inserting data, block ID is derived from checksum but checksum doesn't match", ErrorCodes::LOGICAL_ERROR);
						}

						transaction.rollback();
					}
					else if (zookeeper->exists(quorum_status_path))
					{
						transaction.rollback();

						throw Exception("Another quorum insert has been already started", ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE);
					}
					else
					{
						/// Сюда можем попасть также, если узел с кворумом существовал, но потом быстро был удалён.

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

			if (quorum)
			{
				/// Дожидаемся достижения кворума. TODO Настраиваемый таймаут.
				LOG_TRACE(log, "Waiting for quorum");
				zookeeper->waitForDisappear(quorum_status_path);
				LOG_TRACE(log, "Quorum satisfied");
			}
		}
	}

private:
	StorageReplicatedMergeTree & storage;
	String insert_id;
	size_t quorum;
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
