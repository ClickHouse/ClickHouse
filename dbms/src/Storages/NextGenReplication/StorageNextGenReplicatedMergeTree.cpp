#include <Storages/NextGenReplication/StorageNextGenReplicatedMergeTree.h>
#include <Storages/NextGenReplication/NextGenReplicatedBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeList.h>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/randomSeed.h>
#include <Common/hex.h>

#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int INCORRECT_DATA;
    extern const int REPLICA_IS_ALREADY_EXIST;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int BAD_DATA_PART_NAME;
}

static const auto MERGE_SELECTING_SLEEP_MS = 5 * 1000;


StorageNextGenReplicatedMergeTree::StorageNextGenReplicatedMergeTree(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const String & path_, const String & database_name_, const String & name_,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    Context & context_,
    const ASTPtr & primary_expr_ast_,
    const String & date_column_name,
    const ASTPtr & partition_expr_ast_,
    const ASTPtr & sampling_expression_,
    const MergeTreeData::MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool has_force_restore_data_flag)
    : IStorage(columns_, materialized_columns_, alias_columns_, column_defaults_)
    , context(context_)
    , log(&Logger::get(database_name_ + "." + name_ + " (Storage)"))
    , database_name(database_name_), table_name(name_), full_path(path_ + escapeForFileName(table_name) + '/')
    , data(
        database_name, table_name, full_path,
        columns_, materialized_columns_, alias_columns_, column_defaults_,
        context,
        primary_expr_ast_, date_column_name, partition_expr_ast_, sampling_expression_,
        merging_params_, settings_, true, attach,
        [this] (const String & /* part_name */) { /* TODO: enqueue part for check */; })
    , reader(data), writer(data), merger(data, context.getBackgroundPool())
    , parts_fetcher(data)
    , zookeeper_path(context.getMacros().expand(zookeeper_path_))
    , replica_name(context.getMacros().expand(replica_name_))
{
    /// TODO: unify with StorageReplicatedMergeTree and move to common place
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should starts with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    replica_path = zookeeper_path + "/replicas/" + replica_name;

    bool skip_sanity_checks = has_force_restore_data_flag; /// TODO: load the flag from ZK.

    data.loadDataParts(skip_sanity_checks);

    auto zookeeper = context.getZooKeeper();

    if (!zookeeper)
    {
        if (!attach)
            throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        LOG_ERROR(log, "No ZooKeeper: the table will be in readonly mode.");
        is_readonly = true;
        return;
    }

    if (!attach)
    {
        if (!data.getDataParts().empty())
            throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

        createTableOrReplica(*zookeeper);
    }
    else
    {
        /// Temporary directories contain unfinalized results of Merges or Fetches (after forced restart)
        ///  and don't allow to reinitialize them, so delete each of them immediately.
        data.clearOldTemporaryDirectories(0);

        initPartSet();
    }

    /// TODO: Check table structure.
}

StorageNextGenReplicatedMergeTree::~StorageNextGenReplicatedMergeTree()
{
    try
    {
        shutdown();
    }
    catch(...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageNextGenReplicatedMergeTree::startup()
{
    if (is_readonly)
        return;

    {
        StoragePtr ptr = shared_from_this();
        InterserverIOEndpointPtr endpoint = std::make_shared<DataPartsExchange::Service>(data, ptr);
        parts_exchange_service = std::make_shared<InterserverIOEndpointHolder>(
            endpoint->getId(replica_path), endpoint, context.getInterserverIOHandler());
    }

    {
        auto host_port = context.getInterserverIOAddress();

        /// How other replicas can find us.
        ReplicatedMergeTreeAddress address;
        address.host = host_port.first;
        address.replication_port = host_port.second;
        address.queries_port = context.getTCPPort();
        address.database = database_name;
        address.table = table_name;

        auto zookeeper = getZooKeeper();
        is_active_node = zkutil::EphemeralNodeHolder::create(
            replica_path + "/is_active", *zookeeper, address.toString());

        /// TODO: do something if the node already exists.
    }

    part_set_updating_thread = std::thread([this] { runPartSetUpdatingThread(); });

    parts_producing_task = context.getBackgroundPool().addTask([this] { return runPartsProducingTask(); });

    merge_selecting_thread = std::thread([this] { runMergeSelectingThread(); });
}

void StorageNextGenReplicatedMergeTree::createTableOrReplica(zkutil::ZooKeeper & zookeeper)
{
    auto acl = zookeeper.getDefaultACL();

    if (!zookeeper.exists(zookeeper_path))
    {
        LOG_DEBUG(log, "Creating directory structure in ZooKeeper: " << zookeeper_path);

        zookeeper.createAncestors(zookeeper_path);

        /// TODO: save metadata.
        /// TODO: split parts node by partitions.

        zkutil::Ops ops;
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            zookeeper_path, String(), acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            zookeeper_path + "/parts", String(), acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
            zookeeper_path + "/replicas", String(), acl, zkutil::CreateMode::Persistent));

        auto code = zookeeper.tryMulti(ops);
        if (code != ZOK && code != ZNODEEXISTS)
            throw zkutil::KeeperException(code);
    }

    LOG_DEBUG(log, "Creating replica " << replica_path);
    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
        replica_path, String(), acl, zkutil::CreateMode::Persistent));

    try
    {
        zookeeper.multi(ops);
    }
    catch (const zkutil::KeeperException & e)
    {
        if (e.code == ZNODEEXISTS)
            throw Exception("Replica " + replica_path + " already exists.", ErrorCodes::REPLICA_IS_ALREADY_EXIST);

        throw;
    }
}

void StorageNextGenReplicatedMergeTree::initPartSet()
{
    std::lock_guard<std::mutex> lock(parts_mutex);

    auto parts_on_disk = data.getDataParts(
        {MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});

    for (const MergeTreeData::DataPartPtr & part : parts_on_disk)
    {
        parts.emplace(part->info, Part{part->info, part->name,
            (part->state == MergeTreeDataPartState::Committed) ? Part::State::Committed : Part::State::Outdated});
    }

    /// TODO: check and sync with ZooKeeper.
}

void StorageNextGenReplicatedMergeTree::shutdown()
{
    shutdown_called = true;

    if (merge_selecting_thread.joinable())
    {
        /// TODO: needs protection from concurrent drops.
        merge_selecting_event->set();
        merge_selecting_thread.join();
    }

    parts_fetcher.blocker.cancelForever();

    merger.merges_blocker.cancelForever();

    if (parts_producing_task)
    {
        context.getBackgroundPool().removeTask(parts_producing_task);
        parts_producing_task.reset();
    }

    if (part_set_updating_thread.joinable())
    {
        /// TODO: needs protection from concurrent drops.
        part_set_updating_event->set();
        part_set_updating_thread.join();
    }

    is_active_node.reset();

    if (parts_exchange_service)
    {
        parts_exchange_service->cancelForever();
        parts_exchange_service.reset();
    }
}

BlockInputStreams StorageNextGenReplicatedMergeTree::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    /// TODO: quorum reads

    return reader.read(column_names, query_info, context, processed_stage, max_block_size, num_streams, 0);
}

BlockOutputStreamPtr StorageNextGenReplicatedMergeTree::write(const ASTPtr & /*query*/, const Settings &)
{
    /// TODO: deduplication

    return std::make_shared<NextGenReplicatedBlockOutputStream>(*this);
}

void StorageNextGenReplicatedMergeTree::drop()
{
    {
        auto zookeeper = tryGetZooKeeper();
        if (is_readonly || !zookeeper || zookeeper->expired())
            throw Exception("Can't drop readonly replicated table (need to drop data in ZooKeeper as well)", ErrorCodes::NO_ZOOKEEPER);

        shutdown();

        LOG_INFO(log, "Removing replica " << replica_path << " from ZooKeeper");

        /// TODO: the znodes that indicate that this replica has that part are not deleted.
        /// Maybe get rid of them altogether.
        zookeeper->tryRemoveRecursive(replica_path);

        Strings replicas;
        if (zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) == ZOK && replicas.empty())
        {
            LOG_INFO(log, "Removing table " << zookeeper_path << " from ZooKeeper");
            zookeeper->tryRemoveRecursive(zookeeper_path);
        }
    }

    data.dropAllData();
}

zkutil::ZooKeeperPtr StorageNextGenReplicatedMergeTree::tryGetZooKeeper()
{
    /// TODO: async reconnect?

    return context.tryGetZooKeeper();
}

zkutil::ZooKeeperPtr StorageNextGenReplicatedMergeTree::getZooKeeper()
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    if (res->expired())
        throw Exception("ZooKeeper session expired", ErrorCodes::NO_ZOOKEEPER);
    return res;
}

void StorageNextGenReplicatedMergeTree::runPartSetUpdatingThread()
{
    while (!shutdown_called)
    {
        try
        {
            std::vector<MergeTreePartInfo> ephemeral_parts;
            std::vector<String> active_parts;
            {
                auto zookeeper = getZooKeeper();
                Strings part_nodes = zookeeper->getChildren(
                    zookeeper_path + "/parts", nullptr, part_set_updating_event);

                ActiveDataPartSet current_parts(data.format_version);

                for (const String & part_name : part_nodes)
                {
                    if (startsWith(part_name, "insert_"))
                    {
                        /// TODO extract ephemeral part name creation/parsing code.
                        const char * begin = part_name.data() + strlen("insert_");
                        const char * end = part_name.data() + part_name.length();

                        String partition_id;
                        for (; begin != end && *begin != '_'; ++begin)
                            partition_id.push_back(*begin);

                        if (begin == end)
                            throw Exception("Bad ephemeral insert node name: " + part_name, ErrorCodes::LOGICAL_ERROR);
                        ++begin;

                        UInt64 block_num = parse<UInt64>(begin, end - begin);

                        ephemeral_parts.emplace_back(partition_id, block_num, block_num, 0);
                    }
                    else
                        current_parts.add(part_name);
                }

                active_parts = current_parts.getParts();
            }

            bool have_new_parts = false;
            {
                std::lock_guard<std::mutex> lock(parts_mutex);
                /// TODO maybe not the best idea to lock the mutex for so long.

                for (auto it = parts.begin(); it != parts.end(); )
                {
                    if (it->second.state == Part::State::Ephemeral)
                        it = parts.erase(it);
                    else
                        ++it;
                }

                for (const MergeTreePartInfo & part_info : ephemeral_parts)
                {
                    parts.emplace(
                        part_info, Part{part_info, part_info.getPartName(), Part::State::Ephemeral});
                    /// TODO: calling part_info->getPartName() is a bit misleading here.
                    /// Better not store names in ZK at all.
                    Int64 & low_watermark = low_watermark_by_partition[part_info.partition_id];
                    low_watermark = std::max(low_watermark, part_info.max_block);
                }

                for (const String & part : active_parts)
                {
                    auto part_info = MergeTreePartInfo::fromPartName(part, data.format_version);
                    auto insertion = parts.emplace(part_info, Part{part_info, part, Part::State::Virtual});
                    if (insertion.second)
                    {
                        LOG_TRACE(log, "New part: " << part);
                        have_new_parts = true;
                        Int64 & low_watermark = low_watermark_by_partition[part_info.partition_id];
                        low_watermark = std::max(low_watermark, part_info.max_block);
                    }


                }

            }

            if (have_new_parts)
                parts_producing_task->wake();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            part_set_updating_event->tryWait(1000);
        }

        part_set_updating_event->wait();
    }
}

bool StorageNextGenReplicatedMergeTree::runPartsProducingTask()
{
    thread_local pcg64 rng(randomSeed());

    Part * selected_part = nullptr;
    {
        std::lock_guard<std::mutex> lock(parts_mutex);

        std::vector<Part *> virtual_parts;
        for (auto & part : parts)
        {
            if (part.second.state == Part::State::Virtual)
                virtual_parts.emplace_back(&part.second);
        }

        if (virtual_parts.empty())
            return false;

        selected_part = virtual_parts[rng() % virtual_parts.size()];
        selected_part->state = Part::State::Preparing;
    }

    String part_path = zookeeper_path + "/parts/" + selected_part->name;

    Part::State state_to_revert_to = Part::State::Virtual;
    try
    {
        /// Phase 1: Decide if we should merge a part or download it from a replica.
        bool do_fetch = true;
        String replica_path;
        ReplicatedMergeTreeAddress replica_address;
        MergeTreeData::DataPartsVector parts_to_merge;
        {
            auto zookeeper = getZooKeeper();

            if (selected_part->info.level > 0)
            {
                String part_node_contents = zookeeper->get(part_path);
                ReadBufferFromString merged_parts_buf(part_node_contents);

                bool have_all_parts = true;
                std::lock_guard<std::mutex> lock(parts_mutex);
                while (!merged_parts_buf.eof())
                {
                    String part_name;
                    merged_parts_buf >> part_name >> "\n";
                    auto part_info = MergeTreePartInfo::fromPartName(part_name, data.format_version);

                    auto it = parts.find(part_info);
                    if (it == parts.end())
                    {
                        have_all_parts = false;
                        break;
                    }

                    const Part & part = it->second;
                    if (part.state < Part::State::Prepared || part.state >= Part::State::Outdated)
                    {
                        have_all_parts = false;
                        break;
                    }

                    auto part_ptr = data.getPartIfExists(part_name, {MergeTreeDataPartState::Committed});
                    if (!part_ptr)
                    {
                        have_all_parts = false;
                        break;
                    }

                    parts_to_merge.push_back(std::move(part_ptr));
                }

                /// TODO: more sophisticated heuristics of when to choose fetch.
                if (have_all_parts)
                    do_fetch = false;
                else
                {
                    LOG_DEBUG(log, "Don't have all parts for merge " << selected_part->name
                        << ". Will try to fetch it instead");
                }
            }

            /// Choose a replica from which the part will be fetched.
            if (do_fetch)
            {
                LOG_TRACE(log, "Fetching part " << selected_part->name);

                Strings replicas = zookeeper->getChildren(part_path + "/replicas");
                replicas.erase(
                    std::remove_if(
                        replicas.begin(), replicas.end(),
                        [&](const String & r) { return r == replica_name; }),
                    replicas.end());

                while (!replicas.empty())
                {
                    /// Try to find an active replica
                    size_t i = (rng() % replicas.size());
                    String address;
                    if (zookeeper->tryGet(zookeeper_path + "/replicas/" + replicas[i] + "/is_active", address))
                    {
                        replica_path = zookeeper_path + "/replicas/" + replicas[i];
                        replica_address.fromString(address);
                        break;
                    }
                    else
                        replicas.erase(replicas.begin() + i);
                }

                if (replicas.empty())
                    throw Exception(
                        "No active replica for part " + selected_part->name, ErrorCodes::NO_ACTIVE_REPLICAS);
            }
        }

        /// Phase 2: Execute a merge or a fetch.
        /// TODO: lock table metadata? I don't think it is needed.
        MergeTreeData::MutableDataPartPtr part;
        if (do_fetch)
        {
            auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context.getSettingsRef());
            part = parts_fetcher.fetchPart(
                selected_part->name, replica_path, replica_address.host, replica_address.replication_port, timeouts, false);
        }
        else
        {
            MergeTreeDataMerger::FuturePart future_part(parts_to_merge);
            if (future_part.name != selected_part->name)
                throw Exception(
                    "Future merged part name `" + future_part.name +
                    "` differs from part name in log entry: `" + selected_part->name + "`",
                    ErrorCodes::BAD_DATA_PART_NAME);

            MergeList::EntryPtr merge_entry = context.getMergeList().insert(
                database_name, table_name, selected_part->name, parts_to_merge);
            size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;
            /// TODO: consistent create time for merges.
            time_t merge_time = time(nullptr);
            size_t estimated_space_for_merge = MergeTreeDataMerger::estimateDiskSpaceForMerge(parts_to_merge);
            auto reserved_space = DiskSpaceMonitor::reserve(full_path, estimated_space_for_merge);

            /// TODO: deduplicating merges
            part = merger.mergePartsToTemporaryPart(
                future_part, *merge_entry, aio_threshold, merge_time, reserved_space.get(), /*deduplicate = */ false);
        }

        /// Phase 3: Check the checksum and commit the part.
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

        MergeTreeData::Transaction transaction;
        {
            std::lock_guard<std::mutex> lock(parts_mutex);

            data.renameTempPartAndReplace(part, nullptr, &transaction);
            state_to_revert_to = Part::State::Prepared;
            selected_part->state = state_to_revert_to;
        }

        try
        {
            auto zookeeper = getZooKeeper();

            /// Check or set the checksum.
            String checksum_in_zk;
            bool has_checksum_in_zk = false;
            while (!has_checksum_in_zk)
            {
                has_checksum_in_zk = zookeeper->tryGet(part_path + "/checksum", checksum_in_zk);
                if (has_checksum_in_zk && hash_string != checksum_in_zk)
                {
                    String msg = "Checksum mismatch for part " + selected_part->name;
                    if (do_fetch)
                        msg += " downloaded from " + replica_path;
                    else
                    {
                        msg += " that was merged locally";
                        /// TODO: remember that the merge was unsuccessful and we don't need to repeat
                        /// the merge and need to fetch instead.
                    }
                    throw Exception(msg, ErrorCodes::CHECKSUM_DOESNT_MATCH);
                }

                auto acl = zookeeper->getDefaultACL();
                zkutil::Ops ops;
                if (!has_checksum_in_zk)
                {
                    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                        part_path + "/checksum", hash_string, acl, zkutil::CreateMode::Persistent));
                }
                ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                    part_path + "/replicas/" + replica_name, String(), acl, zkutil::CreateMode::Persistent));

                zkutil::OpResultsPtr op_results;
                auto rc = zookeeper->tryMulti(ops, &op_results);
                if (rc == ZOK)
                {
                    has_checksum_in_zk = true;
                }
                else if (rc == ZNODEEXISTS)
                {
                    if (!has_checksum_in_zk && (*op_results)[0].err == ZNODEEXISTS)
                        continue;
                }
                else
                    throw zkutil::KeeperException(
                        "While trying to add part " + part_path + " to replica " + replica_name, rc);
            }

            {
                std::lock_guard<std::mutex> lock(parts_mutex);
                selected_part->state = Part::State::Committed;
                transaction.commit();
            }
        }
        catch (const zkutil::KeeperException & ex)
        {
            if (zkutil::isTemporaryErrorCode(ex.code))
            {
                std::lock_guard<std::mutex> lock(parts_mutex);
                selected_part->state = Part::State::MaybeCommitted;
                /// TODO: a process that retries adding parts to ZooKeeper.
                transaction.commit();
            }
            else
                throw;
        }
    }
    catch (...)
    {
        std::lock_guard<std::mutex> lock(parts_mutex);
        selected_part->state = state_to_revert_to;
        throw;
    }

    return true;
}

void StorageNextGenReplicatedMergeTree::runMergeSelectingThread()
{
    while (!shutdown_called)
    {
        bool success = false;

        try
        {
            size_t merges_count = 0;
            {
                std::lock_guard<std::mutex> lock(parts_mutex);
                for (const auto & kv : parts)
                {
                    const Part & part = kv.second;
                    if (part.info.level > 0 && part.state < Part::State::Prepared)
                        ++merges_count;
                }
            }

            if (merges_count >= data.settings.max_replicated_merges_in_queue)
            {
                LOG_TRACE(log, "Number of queued merges (" << merges_count
                    << ") is greater than max_replicated_merges_in_queue ("
                    << data.settings.max_replicated_merges_in_queue << "), so won't select new parts to merge.");
            }
            else
            {
                MergeTreeDataMerger::FuturePart future_part;
                size_t max_parts_size_for_merge = merger.getMaxPartsSizeForMerge(
                    data.settings.max_replicated_merges_in_queue, merges_count);

                {
                    std::lock_guard<std::mutex> lock(parts_mutex);

                    /// TODO: merge predicate type is suboptimal here.
                    auto can_merge = [&]
                        (const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right, String *)
                    {
                        auto watermark_it = low_watermark_by_partition.find(left->info.partition_id);
                        if (watermark_it == low_watermark_by_partition.end()
                              || right->info.max_block > watermark_it->second)
                        {
                            return false;
                        }

                        auto left_it = parts.lower_bound(left->info);
                        auto right_it = parts.upper_bound(right->info);
                        for (auto it = left_it; it != right_it; ++it)
                        {
                            if (it->second.state < Part::State::Prepared)
                                return false;
                        }

                        return true;
                    };

                    merger.selectPartsToMerge(future_part, false, max_parts_size_for_merge, can_merge);
                }

                if (!future_part.parts.empty())
                {
                    auto zookeeper = context.getZooKeeper();

                    String parts_path = zookeeper_path + "/parts/";
                    auto acl = zookeeper->getDefaultACL();
                    zkutil::Ops ops;
                    WriteBufferFromOwnString merged_parts_buf;
                    for (const auto & part : future_part.parts)
                    {
                        merged_parts_buf << part->name << '\n';
                        String merged_part_path = parts_path + part->name;
                        ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                            merged_part_path + "/merged", String(), acl, zkutil::CreateMode::Persistent));
                    }

                    /// TODO: separate place for the list merged parts.
                    String part_path = parts_path + future_part.name;
                    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                        part_path, merged_parts_buf.str(), acl, zkutil::CreateMode::Persistent));
                    ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                        part_path + "/replicas", String(), acl, zkutil::CreateMode::Persistent));

                    zkutil::OpResultsPtr op_results;
                    auto rc = zookeeper->tryMulti(ops, &op_results);
                    if (rc == ZOK)
                    {
                        LOG_INFO(log, "Created a part entry " << future_part.name
                            << " that will be merged from " << future_part.parts.size() << " parts "
                            << "(from " << future_part.parts.front()->name
                            << " to " << future_part.parts.back()->name << ")");
                    }
                    else
                    {
                        /// First check for conflicts in the merged parts.
                        bool conflict_found = false;
                        for (size_t i = 0; i < future_part.parts.size(); ++i)
                        {
                            if ((*op_results)[i].err == ZNODEEXISTS)
                            {
                                LOG_DEBUG(log, "Couldn't create a part entry " << future_part.name
                                    << " because of a conflicting op on merged part " << future_part.parts[i]->name);
                                conflict_found = true;
                                break;
                            }
                        }

                        if (!conflict_found)
                            throw zkutil::KeeperException(
                                "Unexpected error while creating a part entry " + future_part.name, rc);
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (shutdown_called)
            break;

        if (!success)
            merge_selecting_event->tryWait(MERGE_SELECTING_SLEEP_MS);
    }
}

}
