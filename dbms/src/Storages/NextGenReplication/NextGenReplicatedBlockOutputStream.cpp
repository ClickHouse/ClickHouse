#include <Storages/NextGenReplication/NextGenReplicatedBlockOutputStream.h>
#include <Interpreters/PartLog.h>
#include <Common/hex.h>

#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STATUS_OF_INSERT;
}


NextGenReplicatedBlockOutputStream::NextGenReplicatedBlockOutputStream(StorageNextGenReplicatedMergeTree & storage_)
    : storage(storage_)
    , log(&Logger::get(storage.database_name + "." + storage.table_name + " (OutputStream)"))
{
}

void NextGenReplicatedBlockOutputStream::write(const Block & block)
{
    storage.data.delayInsertIfNeeded();

    auto zookeeper = storage.getZooKeeper();

    auto part_blocks = storage.writer.splitBlockIntoParts(block);
    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block);

        String parts_path = storage.zookeeper_path + "/parts";

        /// Generate a unique seqnum and ensure that the ephemeral part node is created before any other
        /// part node with the higher seqnum.

        String ephemeral_node_prefix = parts_path + "/insert_" + part->info.partition_id + "_";
        String ephemeral_node_path = zookeeper->create(
            ephemeral_node_prefix, "replica: " + storage.replica_name, zkutil::CreateMode::EphemeralSequential);
        SCOPE_EXIT(
        {
            if (!ephemeral_node_path.empty())
                zookeeper->tryRemoveEphemeralNodeWithRetries(ephemeral_node_path);
        });

        UInt64 block_number = parse<UInt64>(
            ephemeral_node_path.data() + ephemeral_node_prefix.size(),
            ephemeral_node_path.size() - ephemeral_node_prefix.size());

        part->info.min_block = block_number;
        part->info.max_block = block_number;
        part->info.level = 0;
        if (storage.data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
            part->name = part->info.getPartNameV0(part->getMinDate(), part->getMaxDate());
        else
            part->name = part->info.getPartName();

        Part::InProgressGuard inserted_part;
        {
            std::lock_guard<std::mutex> lock(storage.parts_mutex);

            auto insertion = storage.parts.emplace(std::piecewise_construct,
                std::forward_as_tuple(part->info),
                std::forward_as_tuple(part->info, part->name, Part::ReplicationState::Ephemeral));

            if (!insertion.second
                  && insertion.first->second.replication_state != Part::ReplicationState::Ephemeral)
                throw Exception("Part " + part->name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);
            /// TODO check covering and covered parts;

            inserted_part.set(insertion.first->second);
        }

        {
            std::lock_guard<std::mutex> lock(storage.parts_mutex);
            inserted_part.parent->local_part = storage.data.addPreCommittedPart(part);
            if (!inserted_part.parent->local_part)
                throw Exception("Part " + part->name + " is obsolete immediately after insertion.",
                    ErrorCodes::LOGICAL_ERROR);
        }

        SCOPE_EXIT(
        {
            if (inserted_part.parent)
            {
                std::lock_guard<std::mutex> lock(storage.parts_mutex);
                storage.data.removePreCommittedPart(inserted_part.parent->local_part);
                inserted_part.parent->local_part = nullptr;
            }
        });

        String part_path = parts_path + "/" + part->name;

        String hash_string;
        {
            SipHash hash;
            part->checksums.summaryDataChecksum(hash);
            char hash_data[16];
            hash.get128(hash_data);
            hash_string.resize(32);
            for (size_t i = 0; i < 16; ++i)
                writeHexByteLowercase(hash_data[i], &hash_string[2 * i]);
        }

        zkutil::Ops ops;
        auto acl = zookeeper->getDefaultACL();
        ops.emplace_back(std::make_unique<zkutil::Op::Remove>(ephemeral_node_path, -1));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            part_path, String(), acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            part_path + "/checksum", hash_string, acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            part_path + "/replicas", String(), acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            part_path + "/replicas/" + storage.replica_name, String(), acl, zkutil::CreateMode::Persistent));
        try
        {
            zookeeper->multi(ops);
            ephemeral_node_path = String();
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code == ZOPERATIONTIMEOUT || e.code == ZCONNECTIONLOSS)
            {
                {
                    std::lock_guard<std::mutex> lock(storage.parts_mutex);
                    if (inserted_part.parent->replication_state != Part::ReplicationState::None)
                    {
                        storage.data.commitPart(inserted_part.parent->local_part);
                        inserted_part.parent->replication_state = Part::ReplicationState::Committed;
                        inserted_part.parent->status_unknown = true;
                        LOG_TRACE(log, "Committed inserted part " << inserted_part.parent->name);
                    }
                    inserted_part.reset();
                }

                throw Exception("Unknown status, client must retry. Reason: " + e.displayText(),
                    ErrorCodes::UNKNOWN_STATUS_OF_INSERT);
            }

            throw;
        }

        {
            std::lock_guard<std::mutex> lock(storage.parts_mutex);
            if (inserted_part.parent->replication_state != Part::ReplicationState::None)
            {
                storage.data.commitPart(inserted_part.parent->local_part);
                inserted_part.parent->replication_state = Part::ReplicationState::Committed;
                LOG_TRACE(log, "Committed inserted part " << inserted_part.parent->name);
            }
            inserted_part.reset();
        }

        /// TODO: This may not be a good time to invoke merge selector because there may be ephemeral nodes
        /// with earlier block num than the one currently inserted that we know nothing about.
        storage.merge_selecting_event->set();

        PartLog::addNewPartToTheLog(storage.context, *part, watch.elapsed());
    }
}

}
