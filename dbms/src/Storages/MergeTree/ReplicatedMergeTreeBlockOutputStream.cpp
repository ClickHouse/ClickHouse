#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/AbandonableLockInZooKeeper.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Common/SipHash.h>
#include <DB/IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int TOO_LESS_LIVE_REPLICAS;
	extern const int UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE;
	extern const int CHECKSUM_DOESNT_MATCH;
	extern const int UNEXPECTED_ZOOKEEPER_ERROR;
	extern const int NO_ZOOKEEPER;
	extern const int READONLY;
	extern const int UNKNOWN_STATUS_OF_INSERT;
}


ReplicatedMergeTreeBlockOutputStream::ReplicatedMergeTreeBlockOutputStream(
	StorageReplicatedMergeTree & storage_, const String & insert_id_, size_t quorum_, size_t quorum_timeout_ms_)
	: storage(storage_), insert_id(insert_id_), quorum(quorum_), quorum_timeout_ms(quorum_timeout_ms_),
	log(&Logger::get(storage.data.getLogName() + " (Replicated OutputStream)"))
{
	/// Значение кворума 1 имеет такой же смысл, как если он отключён.
	if (quorum == 1)
		quorum = 0;
}


void ReplicatedMergeTreeBlockOutputStream::writePrefix()
{
	/// TODO Можно ли здесь не блокировать структуру таблицы?
	storage.data.delayInsertIfNeeded(&storage.restarting_thread->getWakeupEvent());
}


/// Позволяет проверить, что сессия в ZooKeeper ещё жива.
static void assertSessionIsNotExpired(zkutil::ZooKeeperPtr & zookeeper)
{
	if (!zookeeper)
		throw Exception("No ZooKeeper session.", ErrorCodes::NO_ZOOKEEPER);

	if (zookeeper->expired())
		throw Exception("ZooKeeper session has been expired.", ErrorCodes::NO_ZOOKEEPER);
}


void ReplicatedMergeTreeBlockOutputStream::write(const Block & block)
{
	auto zookeeper = storage.getZooKeeper();

	assertSessionIsNotExpired(zookeeper);

	/** Если запись с кворумом, то проверим, что требуемое количество реплик сейчас живо,
	  *  а также что для всех предыдущих кусков, для которых требуется кворум, этот кворум достигнут.
	  * А также будем проверять, что во время вставки, реплика не была переинициализирована или выключена (по значению узла is_active).
	  * TODO Слишком сложная логика, можно сделать лучше.
	  */
	String quorum_status_path = storage.zookeeper_path + "/quorum/status";
	String is_active_node_value;
	int is_active_node_version = -1;
	int host_node_version = -1;
	if (quorum)
	{
		zkutil::ZooKeeper::TryGetFuture quorum_status_future = zookeeper->asyncTryGet(quorum_status_path);
		zkutil::ZooKeeper::TryGetFuture is_active_future = zookeeper->asyncTryGet(storage.replica_path + "/is_active");
		zkutil::ZooKeeper::TryGetFuture host_future = zookeeper->asyncTryGet(storage.replica_path + "/host");

		/// Список живых реплик. Все они регистрируют эфемерную ноду для leader_election.

		zkutil::Stat leader_election_stat;
		zookeeper->get(storage.zookeeper_path + "/leader_election", &leader_election_stat);

		if (leader_election_stat.numChildren < static_cast<int32_t>(quorum))
			throw Exception("Number of alive replicas ("
				+ toString(leader_election_stat.numChildren) + ") is less than requested quorum (" + toString(quorum) + ").",
				ErrorCodes::TOO_LESS_LIVE_REPLICAS);

		/** Достигнут ли кворум для последнего куска, для которого нужен кворум?
			* Запись всех кусков с включенным кворумом линейно упорядочена.
			* Это значит, что в любой момент времени может быть только один кусок,
			*  для которого нужен, но ещё не достигнут кворум.
			* Информация о таком куске будет расположена в ноде /quorum/status.
			* Если кворум достигнут, то нода удаляется.
			*/

		auto quorum_status = quorum_status_future.get();
		if (quorum_status.exists)
			throw Exception("Quorum for previous write has not been satisfied yet. Status: " + quorum_status.value, ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE);

		/// Обе проверки неявно делаются и позже (иначе был бы race condition).

		auto is_active = is_active_future.get();
		auto host = host_future.get();

		if (!is_active.exists || !host.exists)
			throw Exception("Replica is not active right now", ErrorCodes::READONLY);

		is_active_node_value = is_active.value;
		is_active_node_version = is_active.stat.version;
		host_node_version = host.stat.version;
	}

	auto part_blocks = storage.writer.splitBlockIntoParts(block);

	for (auto & current_block : part_blocks)
	{
		assertSessionIsNotExpired(zookeeper);

		++block_index;
		String block_id = insert_id.empty() ? "" : insert_id + "__" + toString(block_index);
		String month_name = toString(DateLUT::instance().toNumYYYYMMDD(DayNum_t(current_block.min_date)) / 100);

		AbandonableLockInZooKeeper block_number_lock = storage.allocateBlockNumber(month_name);	/// 2 RTT

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
		storage.addNewPartToZooKeeper(part, ops, part_name);

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

			/// Удостоверяемся, что за время вставки, реплика не была переинициализирована или выключена (при завершении сервера).
			ops.push_back(
				new zkutil::Op::Check(
					storage.replica_path + "/is_active",
					is_active_node_version));

			/// К сожалению, одной лишь проверки выше недостаточно, потому что узел is_active может удалиться и появиться заново с той же версией.
			/// Но тогда изменится значение узла host. Будем проверять это.
			/// Замечательно, что эти два узла меняются в одной транзакции (см. MergeTreeRestartingThread).
			ops.push_back(
				new zkutil::Op::Check(
					storage.replica_path + "/host",
					host_node_version));
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
				storage.enqueuePartForCheck(part->name, MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER);

				/// Мы не знаем, были или не были вставлены данные.
				throw Exception("Unknown status, client must retry. Reason: " + e.displayText(), ErrorCodes::UNKNOWN_STATUS_OF_INSERT);
			}

			throw;
		}

		if (quorum)
		{
			/// Дожидаемся достижения кворума.
			LOG_TRACE(log, "Waiting for quorum");

			try
			{
				while (true)
				{
					zkutil::EventPtr event = std::make_shared<Poco::Event>();

					std::string value;
					/// get вместо exists, чтобы не утек watch, если ноды уже нет.
					if (!zookeeper->tryGet(quorum_status_path, value, nullptr, event))
						break;

					ReplicatedMergeTreeQuorumEntry quorum_entry(value);

					/// Если нода успела исчезнуть, а потом появиться снова уже для следующей вставки.
					if (quorum_entry.part_name != part_name)
						break;

					if (!event->tryWait(quorum_timeout_ms))
						throw Exception("Timeout while waiting for quorum");
				}

				/// А вдруг возможно, что текущая реплика в это время перестала быть активной и кворум помечен как неудавшийся, и удалён?
				String value;
				if (!zookeeper->tryGet(storage.replica_path + "/is_active", value, nullptr)
					|| value != is_active_node_value)
					throw Exception("Replica become inactive while waiting for quorum");
			}
			catch (...)
			{
				/// Мы не знаем, были или не были вставлены данные
				/// - успели или не успели другие реплики скачать кусок и пометить кворум как выполненный.
				throw Exception("Unknown status, client must retry. Reason: " + getCurrentExceptionMessage(false),
					ErrorCodes::UNKNOWN_STATUS_OF_INSERT);
			}

			LOG_TRACE(log, "Quorum satisfied");
		}
	}
}


}
