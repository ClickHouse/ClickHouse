#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/AbandonableLockInZooKeeper.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


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
    /// The quorum value `1` has the same meaning as if it is disabled.
    if (quorum == 1)
        quorum = 0;
}


/// Allow to verify that the session in ZooKeeper is still alive.
static void assertSessionIsNotExpired(zkutil::ZooKeeperPtr & zookeeper)
{
    if (!zookeeper)
        throw Exception("No ZooKeeper session.", ErrorCodes::NO_ZOOKEEPER);

    if (zookeeper->expired())
        throw Exception("ZooKeeper session has been expired.", ErrorCodes::NO_ZOOKEEPER);
}


void ReplicatedMergeTreeBlockOutputStream::write(const Block & block)
{
    /// TODO Can I not lock the table structure here?
    storage.data.delayInsertIfNeeded(&storage.restarting_thread->getWakeupEvent());

    auto zookeeper = storage.getZooKeeper();

    assertSessionIsNotExpired(zookeeper);

    /** If write is with quorum, then we check that the required number of replicas is now live,
      *  and also that for all previous pieces for which quorum is required, this quorum is reached.
      * And also check that during the insertion, the replica was not reinitialized or disabled (by the value of `is_active` node).
      * TODO Too complex logic, you can do better.
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

        /// List of live replicas. All of them register an ephemeral node for leader_election.

        zkutil::Stat leader_election_stat;
        zookeeper->get(storage.zookeeper_path + "/leader_election", &leader_election_stat);

        if (leader_election_stat.numChildren < static_cast<int32_t>(quorum))
            throw Exception("Number of alive replicas ("
                + toString(leader_election_stat.numChildren) + ") is less than requested quorum (" + toString(quorum) + ").",
                ErrorCodes::TOO_LESS_LIVE_REPLICAS);

        /** Is there a quorum for the last piece for which a quorum is needed?
            * Write of all the pieces with the included quorum is linearly ordered.
            * This means that at any time there can be only one piece,
            *  for which you need, but not yet reach the quorum.
            * Information about this piece will be located in `/quorum/status` node.
            * If the quorum is reached, then the node is deleted.
            */

        auto quorum_status = quorum_status_future.get();
        if (quorum_status.exists)
            throw Exception("Quorum for previous write has not been satisfied yet. Status: " + quorum_status.value, ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE);

        /// Both checks are implicitly made also later (otherwise there would be a race condition).

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

        AbandonableLockInZooKeeper block_number_lock = storage.allocateBlockNumber(month_name);    /// 2 RTT

        Int64 part_number = block_number_lock.getNumber();

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, part_number);
        String part_name = ActiveDataPartSet::getPartName(part->left_date, part->right_date, part->left, part->right, part->level);

        /// Hash from the data.
        SipHash hash;
        part->checksums.summaryDataChecksum(hash);
        union
        {
            char bytes[16];
            UInt64 words[2];
        } hash_value;
        hash.get128(hash_value.bytes);

        String checksum(hash_value.bytes, 16);

        /// If no ID is specified in query, we take the hash from the data as ID. That is, do not insert the same data twice.
        /// NOTE: If you do not need this deduplication, you can leave `block_id` empty instead.
        ///       Setting or syntax in the query (for example, `ID = null`) could be done for this.
        if (block_id.empty())
        {
            block_id = toString(hash_value.words[0]) + "_" + toString(hash_value.words[1]);

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

        /// Simultaneously add information about the part to all the necessary places in ZooKeeper and remove block_number_lock.

        /// Information about the block.
        zkutil::Ops ops;
        auto acl = zookeeper->getDefaultACL();

        ops.emplace_back(
            std::make_unique<zkutil::Op::Create>(
                storage.zookeeper_path + "/blocks/" + block_id,
                "",
                acl,
                zkutil::CreateMode::Persistent));
        ops.emplace_back(
            std::make_unique<zkutil::Op::Create>(
                storage.zookeeper_path + "/blocks/" + block_id + "/checksum",
                checksum,
                acl,
                zkutil::CreateMode::Persistent));
        ops.emplace_back(
            std::make_unique<zkutil::Op::Create>(
                storage.zookeeper_path + "/blocks/" + block_id + "/number",
                toString(part_number),
                acl,
                zkutil::CreateMode::Persistent));

        /// Information about the part, in the replica data.
        storage.addNewPartToZooKeeper(part, ops, part_name);

        /// Replication log.
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            storage.zookeeper_path + "/log/log-",
            log_entry.toString(),
            acl,
            zkutil::CreateMode::PersistentSequential));

        /// Deletes the information that the block number is used for writing.
        block_number_lock.getUnlockOps(ops);

        /** If you need a quorum - create a node in which the quorum is monitored.
            * (If such a node already exists, then someone has managed to make another quorum record at the same time, but for it the quorum has not yet been reached.
            *  You can not do the next quorum record at this time.)
            */
        if (quorum)
        {
            ReplicatedMergeTreeQuorumEntry quorum_entry;
            quorum_entry.part_name = part_name;
            quorum_entry.required_number_of_replicas = quorum;
            quorum_entry.replicas.insert(storage.replica_name);

            /** At this point, this node will contain information that the current replica received a piece.
                * When other replicas will receive this piece (in the usual way, processing the replication log),
                *  they will add themselves to the contents of this node.
                * When it contains information about `quorum` number of replicas, this node is deleted,
                *  which indicates that the quorum has been reached.
                */

            ops.emplace_back(
                std::make_unique<zkutil::Op::Create>(
                    quorum_status_path,
                    quorum_entry.toString(),
                    acl,
                    zkutil::CreateMode::Persistent));

            /// Make sure that during the insertion time, the replica was not reinitialized or disabled (when the server is finished).
            ops.emplace_back(
                std::make_unique<zkutil::Op::Check>(
                    storage.replica_path + "/is_active",
                    is_active_node_version));

            /// Unfortunately, just checking the above is not enough, because `is_active` node can be deleted and reappear with the same version.
            /// But then the `host` value will change. We will check this.
            /// It's great that these two nodes change in the same transaction (see MergeTreeRestartingThread).
            ops.emplace_back(
                std::make_unique<zkutil::Op::Check>(
                    storage.replica_path + "/host",
                    host_node_version));
        }

        MergeTreeData::Transaction transaction; /// If you can not add a piece to ZK, we'll remove it again from the working set.
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
                /// If the block with such ID already exists in the table, roll back its insertion.
                String expected_checksum;
                if (!block_id.empty() && zookeeper->tryGet(
                    storage.zookeeper_path + "/blocks/" + block_id + "/checksum", expected_checksum))
                {
                    LOG_INFO(log, "Block with ID " << block_id << " already exists; ignoring it (removing part " << part->name << ")");

                    /// If the data is different from the ones that were inserted earlier with the same ID, throw an exception.
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
                    /// if the node with the quorum existed, but was quickly removed.

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
            /** If the connection is lost, and we do not know if the changes were applied, you can not delete the local chunk
                *  if the changes were applied, the inserted block appeared in `/blocks/`, and it can not be inserted again.
                */
            if (e.code == ZOPERATIONTIMEOUT ||
                e.code == ZCONNECTIONLOSS)
            {
                transaction.commit();
                storage.enqueuePartForCheck(part->name, MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER);

                /// We do not know whether or not data has been inserted.
                throw Exception("Unknown status, client must retry. Reason: " + e.displayText(), ErrorCodes::UNKNOWN_STATUS_OF_INSERT);
            }

            throw;
        }

        if (quorum)
        {
            /// We are waiting for the quorum to be reached.
            LOG_TRACE(log, "Waiting for quorum");

            try
            {
                while (true)
                {
                    zkutil::EventPtr event = std::make_shared<Poco::Event>();

                    std::string value;
                    /// `get` instead of `exists` so that `watch` does not leak if the node is no longer there.
                    if (!zookeeper->tryGet(quorum_status_path, value, nullptr, event))
                        break;

                    ReplicatedMergeTreeQuorumEntry quorum_entry(value);

                    /// If the node has time to disappear, and then appear again for the next insert.
                    if (quorum_entry.part_name != part_name)
                        break;

                    if (!event->tryWait(quorum_timeout_ms))
                        throw Exception("Timeout while waiting for quorum");
                }

                /// And what if it is possible that the current replica at this time has ceased to be active and the quorum is marked as failed and deleted?
                String value;
                if (!zookeeper->tryGet(storage.replica_path + "/is_active", value, nullptr)
                    || value != is_active_node_value)
                    throw Exception("Replica become inactive while waiting for quorum");
            }
            catch (...)
            {
                /// We do not know whether or not data has been inserted
                /// - whether other replicas have time to download the part and mark the quorum as done.
                throw Exception("Unknown status, client must retry. Reason: " + getCurrentExceptionMessage(false),
                    ErrorCodes::UNKNOWN_STATUS_OF_INSERT);
            }

            LOG_TRACE(log, "Quorum satisfied");
        }
    }
}


}
