#include <Storages/NextGenReplication/NextGenReplicatedBlockOutputStream.h>
#include <Storages/NextGenReplication/StorageNextGenReplicatedMergeTree.h>
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

        String ephemeral_node_prefix = parts_path + "/tmp_insert_";
        String ephemeral_node_path = zookeeper->create(
            ephemeral_node_prefix, String(), zkutil::CreateMode::EphemeralSequential);
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
            part_path, storage.replica_name, acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            part_path + "/checksum", hash_string, acl, zkutil::CreateMode::Persistent));

        MergeTreeData::Transaction transaction;
        storage.data.renameTempPartAndAdd(part, nullptr, &transaction);

        try
        {
            zookeeper->multi(ops);
            ephemeral_node_path = String();
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code == ZOPERATIONTIMEOUT || e.code == ZCONNECTIONLOSS)
            {
                transaction.commit();
                /// TODO: mark that the status is unknown.
                throw Exception("Unknown status, client must retry. Reason: " + e.displayText(),
                    ErrorCodes::UNKNOWN_STATUS_OF_INSERT);
            }

            throw;
        }

        transaction.commit();
        storage.merge_selecting_event.set();

        PartLog::addNewPartToTheLog(storage.context, *part, watch.elapsed());
    }
}

}
