#include <Storages/MergeTree/ReplicatedMergeTreeCurrentlyRestoringFromBackup.h>

#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Storages/MergeTree/MutationInfoFromBackup.h>
#include <Storages/MergeTree/calculateBlockNumbersForRestoring.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Backups/WithRetries.h>
#include <base/insertAtEnd.h>

#include <Poco/UUIDGenerator.h>
#include <boost/algorithm/string/join.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
    extern const int TABLE_IS_READ_ONLY;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
    extern const int NO_ZOOKEEPER;
}


namespace
{
    /// There can be various reasons to check that no parts exist.
    enum class NoPartsCheckReason
    {
        /// No need to check for no parts.
        NONE,

        /// Non-empty tables are not allowed because of the restore command's settings: `SETTINGS allow_non_empty_tables = false`.
        NON_EMPTY_TABLE_IS_NOT_ALLOWED,

        /// We going to restore mutations, and mutations from a backup should never be applied to existing parts.
        RESTORING_MUTATIONS,
    };

    /// Keeps a list of paths to zookeeper nodes, which are removed in the destructor.
    class TemporaryZookeeperNodes
    {
    public:
        TemporaryZookeeperNodes() = default;
        explicit TemporaryZookeeperNodes(const ZooKeeperWithFaultInjectionPtr & zookeeper_) : zookeeper(zookeeper_) { }
        TemporaryZookeeperNodes(TemporaryZookeeperNodes && src) noexcept { *this = std::move(src); }
        ~TemporaryZookeeperNodes() { tryRemoveNodesNoThrow(); }

        TemporaryZookeeperNodes & operator=(TemporaryZookeeperNodes && src) noexcept
        {
            if (this == &src)
                return *this;
            tryRemoveNodesNoThrow();
            if (!zookeeper)
                zookeeper = src.zookeeper;
            paths = std::move(src.paths);
            src.paths.clear();
            return *this;
        }

        const Strings & getPaths() const { return paths; }

        void addPath(const String & path_) { paths.emplace_back(path_); }
        void addPaths(const Strings & paths_) { std::copy(paths_.begin(), paths_.end(), std::inserter(paths, paths.end())); }

        size_t size() const { return paths.size(); }

        void join(TemporaryZookeeperNodes && other)
        {
            if (!zookeeper)
                zookeeper = other.zookeeper;
            insertAtEnd(paths, other.paths);
            other.paths.clear();
        }

        void removeNodes()
        {
            if (paths.empty())
                return;

            if (!zookeeper)
                    throw Exception(ErrorCodes::NO_ZOOKEEPER, "Cannot get ZooKeeper");

            std::vector<zkutil::ZooKeeper::FutureMulti> futures;

            size_t pos = 0;
            while (pos < paths.size())
            {
                size_t count = std::min(paths.size() - pos, zkutil::MULTI_BATCH_SIZE);
                Coordination::Requests ops;
                for (size_t i = 0; i != count; ++i)
                    ops.push_back(zkutil::makeRemoveRequest(paths[pos++], -1));

                futures.emplace_back(zookeeper->asyncMulti(ops));
            }

            for (auto & future : futures)
                future.get();

            paths.clear();
        }

        void tryRemoveNodesNoThrow() noexcept
        {
            if (paths.empty() || !zookeeper)
                return;

            try
            {
                std::vector<zkutil::ZooKeeper::FutureRemove> futures;
                futures.reserve(paths.size());
                for (const auto & path : paths)
                    futures.push_back(zookeeper->asyncTryRemoveNoThrow(path));

                for (auto & future : futures)
                    future.get();

                paths.clear();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

    private:
        std::vector<String> paths;
        ZooKeeperWithFaultInjectionPtr zookeeper;
    };
}


/// Creates temporary zookeeper nodes to allocate block numbers and mutation numbers.
class ReplicatedMergeTreeCurrentlyRestoringFromBackup::BlockNumbersAllocator
{
public:
    explicit BlockNumbersAllocator(
        StorageReplicatedMergeTree & storage_, Poco::Logger * log_, CurrentlyRestoringInfo & currently_restoring_info_)
        : storage(storage_), log(log_), currently_restoring_info(currently_restoring_info_)
    {
    }

    /// Allocates block numbers and mutation numbers and recalculates `part_infos_` and `mutation_infos_`.
    /// The function returns a list of zookeeper paths to ephemeral nodes created in "/block_numbers/" and "/mutations".
    TemporaryZookeeperNodes allocateBlockNumbers(
        std::vector<MergeTreePartInfo> & part_infos_,
        std::vector<MutationInfoFromBackup> & mutation_infos_,
        NoPartsCheckReason no_parts_check_reason_,
        const ZooKeeperWithFaultInjectionPtr & zookeeper_,
        const ContextPtr & context_) const
    {
        /// Check the table has no existing parts if it's necessary.
        /// If a backup contains mutations the table must have no existing parts,
        /// otherwise mutations from the backup could be applied to existing parts which is wrong.
        if (no_parts_check_reason_ != NoPartsCheckReason::NONE)
            checkNoPartsExist(zookeeper_, {}, no_parts_check_reason_);

        LOG_INFO(log, "Creating ephemeral nodes to allocate block numbers and mutation numbers for {} parts and {} mutations",
                 part_infos_.size(), mutation_infos_.size());

        /// Temporary nodes will be removed later (see ReplicatedMergeTreeCurrentlyRestoringFromBackup::allocateBlockNumbers).
        TemporaryZookeeperNodes temp_nodes;

        size_t num_partitions = 0;
        size_t total_num_block_numbers = 0;

        /// These two `do_allocate*` functions will be called from calculateBlockNumbersForRestoringReplicatedMergeTree() below.
        auto do_allocate_block_numbers = [&](const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers)
        {
            /// Create partition nodes if they were not created before.
            num_partitions = partitions_and_num_block_numbers.size();
            createPartitionNodes(partitions_and_num_block_numbers, zookeeper_);

            /// Create block number nodes in all partitions.
            auto block_number_nodes = createBlockNumberNodes(partitions_and_num_block_numbers, zookeeper_);

            /// Extract block numbers from the name of the block number nodes.
            std::vector<BlockNumbers> block_numbers;
            block_numbers.resize(num_partitions);

            for (size_t i = 0; i != num_partitions; ++i)
            {
                const String & partition_id = partitions_and_num_block_numbers[i].partition_id;
                block_numbers[i] = extractBlockNumbersFromPaths(block_number_nodes[i], partition_id);

                if (no_parts_check_reason_ != NoPartsCheckReason::NONE)
                    checkBlockNumbersSequential(no_parts_check_reason_, partition_id, block_numbers[i]);

                total_num_block_numbers += block_numbers[i].size();
                temp_nodes.join(std::exchange(block_number_nodes[i], {}));
            }

            return block_numbers;
        };

        auto do_allocate_mutation_numbers = [&](size_t num_mutations)
        {
            auto mutation_number_nodes = createMutationNumberNodes(num_mutations, zookeeper_);
            auto mutation_numbers = extractMutationNumbersFromPaths(mutation_number_nodes);
            temp_nodes.join(std::exchange(mutation_number_nodes, {}));
            return mutation_numbers;
        };

        auto metadata_snapshot = storage.getInMemoryMetadataPtr();
        auto get_partitions_affected_by_mutation = [&](const MutationCommands & commands)
        {
            return storage.getPartitionIdsAffectedByCommands(commands, context_);
        };

        /// Calculate block numbers using the `do_allocate*` functions.
        calculateBlockNumbersForRestoringReplicatedMergeTree(
            part_infos_, mutation_infos_, do_allocate_block_numbers, do_allocate_mutation_numbers, get_partitions_affected_by_mutation);

        /// Check the table has no existing parts again to be sure no parts were added while we were allocating block numbers.
        if (no_parts_check_reason_ != NoPartsCheckReason::NONE)
            checkNoPartsExist(zookeeper_, temp_nodes.getPaths(), no_parts_check_reason_);

        LOG_INFO(log, "{} ephemeral nodes created to allocate {} block numbers in {} partitions and {} mutation numbers",
            temp_nodes.size(), total_num_block_numbers, num_partitions, mutation_infos_.size());

        return temp_nodes;
    }

private:
    StorageReplicatedMergeTree & storage;
    Poco::Logger * const log;
    CurrentlyRestoringInfo & currently_restoring_info;

    String getTableName() const { return storage.getStorageID().getFullTableName(); }

    /// Creates nodes "block_numbers/<partition_id>" for each partition. Some of these nodes can already exist.
    void createPartitionNodes(
        const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers,
        const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        size_t num_partitions = partitions_and_num_block_numbers.size();
        if (!num_partitions)
            return;

        String block_numbers_path = fs::path(storage.zookeeper_path) / "block_numbers";

        std::vector<Coordination::Requests> ops;
        std::vector<std::future<Coordination::MultiResponse>> futures;
        ops.resize(num_partitions);
        futures.resize(num_partitions);

        /// Make requests, run then asynchronously.
        for (size_t i = 0; i != num_partitions; ++i)
        {
            const String & partition_id = partitions_and_num_block_numbers[i].partition_id;
            ops[i] = storage.getPartitionNodeCreateOps(partition_id);
            futures[i] = zookeeper->asyncTryMultiNoThrow(ops[i]);
        }

        /// Check the results of those requests.
        for (size_t i = 0; i != num_partitions; ++i)
        {
            if (!ops[i].empty()) /// `ops` is empty if the partition node already exist
            {
                auto response = futures[i].get();
                auto code = response.error;
                if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                    zkutil::KeeperMultiException::check(code, ops[i], response.responses);
            }
        }
    }

    /// Creates multiple sequential ephemeral nodes like "block_numbers/<partition_id>/block-0000000321" to allocate block numbers.
    std::vector<TemporaryZookeeperNodes> createBlockNumberNodes(
        const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers,
        const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        return createBlockNumberNodesImpl(
            partitions_and_num_block_numbers, String{fs::path(storage.zookeeper_path) / "block_numbers"} + "/", "/block-", zookeeper);
    }

    /// A generalized version of createBlockNumberNodes() so we can use it to create mutation numbers too.
    std::vector<TemporaryZookeeperNodes> createBlockNumberNodesImpl(
        const std::vector<PartitionIDAndNumBlockNumbers> & partitions_and_num_block_numbers,
        const String & path_part_before_partition_id,
        const String & path_part_after_partition_id,
        const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        size_t num_partitions = partitions_and_num_block_numbers.size();
        if (!num_partitions)
            return {};

        std::vector<TemporaryZookeeperNodes> res;
        res.reserve(num_partitions);
        for (size_t i = 0; i != num_partitions; ++i)
            res.emplace_back(TemporaryZookeeperNodes{zookeeper});

        Coordination::Requests ops;
        std::vector<size_t> indices;
        std::vector<std::future<Coordination::MultiResponse>> futures;
        indices.reserve(num_partitions);
        futures.reserve(num_partitions);

        /// There can be many block numbers in many partitions.
        /// For better performance we use asyncMulti() to create up to 100 block numbers in each partition,
        /// then we wait for those operations to complete, and after that we repeat.

        do
        {
            indices.clear();
            futures.clear();

            /// Start asyncMulti() for each partitions we still need block numbers.
            for (size_t i = 0; i != num_partitions; ++i)
            {
                const String & partition_id = partitions_and_num_block_numbers[i].partition_id;
                size_t num_block_numbers = partitions_and_num_block_numbers[i].num_block_numbers;

                num_block_numbers -= res[i].size();
                num_block_numbers = std::min(num_block_numbers, zkutil::MULTI_BATCH_SIZE - 1);

                if (!num_block_numbers)
                    continue;

                indices.emplace_back(i);

                String full_prefix = path_part_before_partition_id + partition_id + path_part_after_partition_id;

                ops.clear();
                ops.reserve(num_block_numbers + 1); /// the last request is a check request

                for (size_t j = 0; j != num_block_numbers; ++j)
                    ops.push_back(zkutil::makeCreateRequest(full_prefix, "", zkutil::CreateMode::EphemeralSequential));

                /// Check that table is not being dropped ("host" is the first node that is removed on replica drop)
                ops.push_back(zkutil::makeCheckRequest(fs::path(storage.replica_path) / "host", -1));

                futures.emplace_back(zookeeper->asyncMulti(ops));
            }

            /// Wait for asyncMulti() operations to complete, collect the created paths.
            for (size_t i = 0; i != futures.size(); ++i)
            {
                const auto & responses = futures[i].get().responses;
                auto & temp_nodes = res[indices[i]];

                if (responses.size() < 2)
                    throw Exception(ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR, "Number of responses for the multi request must >= 2");

                size_t num_create_requests = responses.size() - 1; /// the last request is a check request
                for (size_t j = 0; j != num_create_requests; ++j)
                {
                    const auto * response = dynamic_cast<const Coordination::CreateResponse *>(responses[j].get());
                    if (!response)
                        throw Exception(ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR, "Response for a create request must have type CreateResponse");

                    temp_nodes.addPath(response->path_created);
                }
            }
        } while (!futures.empty());

        /// No partition needs more block numbers, everything is ready.
        return res;
    }

    /// Creates multiple sequential ephemeral nodes like "mutations/mutation-placeholder-0000000321" to allocate mutation numbers.
    TemporaryZookeeperNodes createMutationNumberNodes(size_t num_mutations, const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        auto temp_nodes = createBlockNumberNodesImpl(
            {PartitionIDAndNumBlockNumbers{"", num_mutations}},
            fs::path(storage.zookeeper_path) / "mutations" / kMutationPlaceholderPrefix,
            "",
            zookeeper);
        return std::move(temp_nodes[0]);
    }

    /// Extracts block numbers from the endings of the paths to multiple sequential nodes created to allocate block numbers
    /// like "block_numbers/<partition_id>/block-0000000321".
    std::vector<Int64> extractBlockNumbersFromPaths(const TemporaryZookeeperNodes & block_number_nodes, const String & partition_id) const
    {
        auto block_numbers_path = fs::path(storage.zookeeper_path) / "block_numbers";
        auto prefix = block_numbers_path / partition_id / "block-";
        return extractNumbersFromSequentialNodePaths(block_number_nodes.getPaths(), prefix);
    }

    /// Extracts mutation numbers from the endings of the paths to multiple sequential ephemeral nodes creates to allocate mutation numbers
    /// like "mutations/mutation-placeholder-0000000321".
    std::vector<Int64> extractMutationNumbersFromPaths(const TemporaryZookeeperNodes & mutation_number_nodes) const
    {
        auto mutations_path = fs::path(storage.zookeeper_path) / "mutations";
        auto prefix = mutations_path / kMutationPlaceholderPrefix;
        return extractNumbersFromSequentialNodePaths(mutation_number_nodes.getPaths(), prefix);
    }

    /// Extracts numbers from the endings of multiple sequential node paths.
    std::vector<Int64> extractNumbersFromSequentialNodePaths(const Strings & zookeeper_paths, const String & prefix) const
    {
        std::vector<Int64> res;
        res.reserve(zookeeper_paths.size());
        for (const String & zookeeper_path : zookeeper_paths)
            res.emplace_back(extractNumberFromSequentialNodePath(zookeeper_path, prefix));
        return res;
    }

    /// Extracts a number from the endings of a sequential node path.
    Int64 extractNumberFromSequentialNodePath(const String & zookeeper_path, const String & prefix) const
    {
        if (!zookeeper_path.starts_with(prefix))
        {
            throw Exception(ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER, "Sequential node '{}' was created without prefix '{}'",
                            zookeeper_path, prefix);
        }
        return parseFromString<Int64>(zookeeper_path.substr(prefix.length()));
    }

    /// Tries to extract a number from the endings of a sequential node path without throwing exceptions.
    std::optional<Int64> tryExtractNumberFromSequentialNodePath(const String & zookeeper_path, const String & prefix) const
    {
        if (!zookeeper_path.starts_with(prefix))
            return {};

        Int64 res;
        if (!tryParse(res, zookeeper_path.substr(prefix.length())))
            return {};

        return res;
    }

    /// Checks that there no parts exist in the table.
    /// The function also checks that no parts will be inserted / attached soon due to a concurrent process.
    void checkNoPartsExist(const ZooKeeperWithFaultInjectionPtr & zookeeper, const Strings & ignore_block_number_zk_paths, NoPartsCheckReason reason) const
    {
        if (reason == NoPartsCheckReason::NONE)
            return;

        LOG_INFO(log, "Checking that no parts exist in the table before restoring its data (reason: {})", magic_enum::enum_name(reason));

        /// The order of checks is important:
        /// an INSERT command first allocates a block number, then it creates a log entry, and finally it replicates the part into all replicas.
        /// That's why it's more reliable to check first block numbers, then the replication queue and finally the current replica itself.

        auto allocated_block_number = findAnyAllocatedBlockNumber(zookeeper, ignore_block_number_zk_paths);

        auto part_name_in_queue = findAnyPartInReplicationQueue(zookeeper);

        if (auto part_name_on_replica = findAnyPartOnReplica())
            throwTableIsNotEmpty(reason, fmt::format("part {} exists", *part_name_on_replica));

        if (part_name_in_queue)
            throwTableIsNotEmpty(reason, fmt::format("part {} exists on some replicas", *part_name_in_queue));

        if (auto restoring_part_name = findAnyRestoringPart(zookeeper))
            throwTableIsNotEmpty(reason, fmt::format("concurrent RESTORE is creating part {}", *restoring_part_name));

        if (allocated_block_number)
            throwTableIsNotEmpty(reason, fmt::format("concurrent INSERT or ATTACH is using block number {} in partition {}", allocated_block_number->first, allocated_block_number->second));
    }

    /// Finds any part on the current replica.
    std::optional<String> findAnyPartOnReplica() const
    {
        if (storage.getTotalActiveSizeInBytes() == 0)
            return {};

        auto parts_lock = storage.lockParts();

        auto parts = storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active, MergeTreeDataPartState::PreActive}, parts_lock);
        if (parts.empty())
            return {};

        return parts.front()->name;
    }

    /// Finds any part in the replication queue.
    std::optional<String> findAnyPartInReplicationQueue(const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        storage.queue.pullLogsToQueue(zookeeper, {}, ReplicatedMergeTreeQueue::CHECK_EMPTY);

        auto queue_status = storage.queue.getStatus();
        if (!queue_status.inserts_in_queue)
            return {};

        ReplicatedMergeTreeQueue::LogEntriesData log_entries;
        storage.queue.getEntries(log_entries);

        for (const auto & log_entry : log_entries)
        {
            if (log_entry.type == ReplicatedMergeTreeLogEntryData::GET_PART || log_entry.type == ReplicatedMergeTreeLogEntryData::ATTACH_PART)
                return log_entry.new_part_name;
        }

        return {};
    }

    /// Finds any locked block number excluding ones we have just allocated.
    std::optional<std::pair<Int64, String>> findAnyAllocatedBlockNumber(const ZooKeeperWithFaultInjectionPtr & zookeeper, const Strings & ignore_zk_paths) const
    {
        auto block_numbers_path = fs::path(storage.zookeeper_path) / "block_numbers";
        Strings partition_ids = zookeeper->getChildren(block_numbers_path);

        std::unordered_set<std::string_view> ignore_zk_paths_set(ignore_zk_paths.begin(), ignore_zk_paths.end());

        for (const String & partition_id : partition_ids)
        {
            Strings paths = zookeeper->getChildren(block_numbers_path / partition_id);
            for (const String & path : paths)
            {
                if (ignore_zk_paths_set.contains(path))
                    continue;

                auto prefix = block_numbers_path / partition_id / "block-";
                auto block_number = tryExtractNumberFromSequentialNodePath(path, prefix);
                if (block_number)
                    return std::make_pair(*block_number, partition_id);
            }
        }

        return {};
    }

    /// Finds any part which is already restoring by a concurrent RESTORE.
    std::optional<String> findAnyRestoringPart(const ZooKeeperWithFaultInjectionPtr & zookeeper) const;

    /// If some block numbers were skipped during allocation that means there was a concurrent INSERT which allocated some block numbers for itself.
    /// And concurrent INSERTs are prohibited if we check that no parts exist.
    void checkBlockNumbersSequential(NoPartsCheckReason reason, const String & partition_id, const std::vector<Int64> & block_numbers) const
    {
        if ((reason == NoPartsCheckReason::NONE) || (block_numbers.size() <= 1))
            return;

        for (size_t i = 1; i != block_numbers.size(); ++i)
        {
            Int64 expected_block_number = block_numbers[0] + i;
            if (block_numbers[i] != expected_block_number)
                throwTableIsNotEmpty(reason, fmt::format("concurrent INSERT or ATTACH is using block number {} in partition {}",
                                                         expected_block_number, partition_id));
        }
    }

    /// Provides an extra description for error messages to make them more detailed.
    [[noreturn]] void throwTableIsNotEmpty(NoPartsCheckReason reason, const String & details) const
    {
        String message = fmt::format("Cannot restore the table {} because it already contains some data{}.",
                                     getTableName(), details.empty() ? "" : (" (" + details + ")"));
        if (reason == NoPartsCheckReason::NON_EMPTY_TABLE_IS_NOT_ALLOWED)
            message += "You can either truncate the table before restoring OR set \"allow_non_empty_tables=true\" to allow appending to "
                         "existing data in the table";
        else if (reason == NoPartsCheckReason::RESTORING_MUTATIONS)
            message += "Mutations cannot be restored if a table is non-empty already. You can either truncate the table before "
                         "restoring OR set \"restore_mutations=false\" to restore the table without restoring its mutations";
        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "{}", message);
    }
};


/// Keeps information about currently restoring parts and mutations in ZooKeeper so other replicas can use it too.
class ReplicatedMergeTreeCurrentlyRestoringFromBackup::CurrentlyRestoringInfo
{
public:
    explicit CurrentlyRestoringInfo(StorageReplicatedMergeTree & storage_, Poco::Logger * log_)
        : storage(storage_), log(log_), serialization_root_path(fs::path(storage_.zookeeper_path) / "currently_restoring_from_backup")
    {
    }

    /// Whether this part is being restored from a backup?
    bool containsPart(const MergeTreePartInfo & part_info) const
    {
        std::lock_guard lock{mutex};
        for (const auto & [_, entry] : entries)
        {
            if (entry.part_infos.contains(part_info))
                return true;
        }
        return false;
    }

    /// Finds any part which is already restoring.
    std::optional<String> findAnyRestoringPart() const
    {
        std::lock_guard lock{mutex};
        for (const auto & [_, entry] : entries)
        {
            if (!entry.part_infos.empty())
                return entry.part_infos.begin()->getPartNameAndCheckFormat(storage.format_version);
        }
        return {};
    }

    /// Whether this mutation is being restored from a backup?
    bool containsMutation(const String & mutation_name) const
    {
        std::lock_guard lock{mutex};
        for (const auto & [_, entry] : entries)
        {
            if (entry.mutation_names.contains(mutation_name))
                return true;
        }
        return false;
    }

    /// Stores information about parts and mutations we're going to restore in memory and ZooKeeper.
    /// A returned `scope_guard` is used to control how long this information should be kept.
    scope_guard addEntry(
        const std::vector<MergeTreePartInfo> & part_infos_,
        const std::vector<MutationInfoFromBackup> & mutation_infos_,
        String & zookeeper_path_for_checking_,
        const ZooKeeperWithFaultInjectionPtr & zookeeper_,
        const WithRetries::KeeperSettings & keeper_settings_)
    {
        /// Make an entry to keep information about parts and mutations being restored.
        Entry entry;

        std::copy(part_infos_.begin(), part_infos_.end(), std::inserter(entry.part_infos, entry.part_infos.end()));

        for (const auto & mutation_info : mutation_infos_)
            entry.mutation_names.insert(mutation_info.name);

        entry.replica_name = storage.replica_name;
        entry.writing_to_zookeeper = true;

        String entry_id = generateEntryID();
        String entry_as_string = entryToString(entry);
        Strings entry_as_string_parts = splitIntoParts(entry_as_string, keeper_settings_.keeper_value_max_size);
        Strings serialization_paths = getSerializationPaths(entry_id, entry_as_string_parts.size());
        entry.serialization_paths = serialization_paths;

        /// This `scope_guard` is used to remove the entry when we're done with the current RESTORE process.
        scope_guard remove_entry = [this,
                                    entry_id,
                                    serialization_paths,
                                    keeper_fault_injection_probability = keeper_settings_.keeper_fault_injection_probability,
                                    keeper_fault_injection_seed = keeper_settings_.keeper_fault_injection_seed]
        {
            {
                /// Remove from memory.
                LOG_INFO(log, "Removing info about currently restoring parts");
                std::lock_guard lock{mutex};
                entries.erase(entry_id);
            }

            try
            {
                /// Remove from ZooKeeper.
                auto zookeeper = ZooKeeperWithFaultInjection::createInstance(
                    keeper_fault_injection_probability, keeper_fault_injection_seed, storage.tryGetZooKeeper(), log->name(), log);
                tryRemoveEntryFromZookeeper(serialization_paths, zookeeper);
            }
            catch (...)
            {
                tryLogCurrentException(log,
                                       fmt::format("Failed to remove info about currently restoring parts from ZooKeeper from {}",
                                                   boost::algorithm::join(serialization_paths, ", ")));
            }
        };

        {
            /// Store the entry in memory.
            LOG_INFO(log, "Storing info about currently restoring parts");
            std::lock_guard lock{mutex};
            entries.emplace(entry_id, std::move(entry));
        }

        /// Store the entry in ZooKeeper.
        saveEntryToZookeeper(serialization_paths, entry_as_string_parts, zookeeper_);

        {
            /// Mark that the entry was written to ZooKeeper.
            std::lock_guard lock{mutex};
            auto it = entries.find(entry_id);
            if (it != entries.end())
                it->second.writing_to_zookeeper = false;
        }

        /// A path in ZooKeeper which can be used to check if the parts and mutations are restored or should be restored.
        zookeeper_path_for_checking_ = serialization_paths.front();

        return remove_entry;
    }

    /// Rereads information about currently restoring parts from ZooKeeper.
    void update(const ZooKeeperWithFaultInjectionPtr & zookeeper, Coordination::WatchCallback watch_callback = {})
    {
        LOG_INFO(log, "Updating info about currently restoring parts from ZooKeeper");

        /// No simultaneous updates.
        std::lock_guard lock_update_mutex{update_mutex};

        /// Read from ZooKeeper a list of children of the `serialization_root_path` node and parse those paths.
        /// Then we compare `update_infos` (those paths parsed) with `entries` (i.e. we compare entries in ZooKeeper and entries in memory).
        auto update_infos = readUpdateInfos(zookeeper, watch_callback);

        Strings nodes_to_remove;
        std::vector<std::pair<String, UpdateInfo>> update_infos_with_new_entries;

        {
            std::lock_guard lock{mutex};

            Strings entry_ids_to_remove;
            for (const auto & [entry_id, entry] : entries)
            {
                if (!entry.writing_to_zookeeper && !update_infos.contains(entry_id))
                {
                    LOG_INFO(log, "Info about currently restoring parts was removed from ZooKeeper, removing it from memory too");
                    entry_ids_to_remove.emplace_back(entry_id);
                }
            }

            /// If an entry exists in memory but it doesn't exist in ZooKeeper then probably it was removed by another replica so
            /// we need to remove it from memory.
            for (const auto & entry_id : entry_ids_to_remove)
                entries.erase(entry_id);

            for (const auto & [entry_id, update_info] : update_infos)
            {
                if (!entries.contains(entry_id))
                {
                    if (update_info.replica_name == storage.replica_name)
                    {
                        LOG_INFO(log, "Info about currently restoring parts was removed from memory, removing it from ZooKeeper too");
                        insertAtEnd(nodes_to_remove, update_info.serialization_paths);
                    }
                    else if (update_info.complete)
                    {
                        LOG_INFO(log, "Found new info about currently restoring parts in ZooKeeper, loading it to memory");
                        update_infos_with_new_entries.emplace_back(entry_id, update_info);
                    }
                }
            }
        }

        /// If an entry for this replica exists in ZooKeeper but it doesn't exist in memory there was an error and so the entry wasn't removed
        /// from ZooKeeper because of that error. So we can try to remove it from ZooKeeper now.
        if (!nodes_to_remove.empty())
            tryRemoveEntryFromZookeeper(nodes_to_remove, zookeeper);

        /// If an entry for another replica exists in ZooKeeper but it doesn't exist in memory then it was written by another replica so
        /// we need to read it from ZooKeeper.
        std::vector<std::pair<String, Entry>> new_entries;
        new_entries.reserve(update_infos_with_new_entries.size());
        for (const auto & [entry_id, update_info] : update_infos_with_new_entries)
        {
            auto entry = loadEntryFromZookeeper(update_info.serialization_paths, zookeeper);
            if (entry)
                new_entries.emplace_back(entry_id, *std::move(entry));
        }

        {
            std::lock_guard lock{mutex};
            for (auto & [entry_id, entry] : new_entries)
                entries.emplace(entry_id, std::move(entry));
        }
    }

private:
    StorageReplicatedMergeTree & storage;
    Poco::Logger * const log;
    const String serialization_root_path;

    /// Represents parts and mutations restored by one RESTORE command to this table.
    struct Entry
    {
        std::set<MergeTreePartInfo> part_infos;
        std::unordered_set<String> mutation_names;
        String replica_name;
        bool writing_to_zookeeper = false;
        Strings serialization_paths;
    };

    std::unordered_map<String, Entry> entries TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;

    std::mutex update_mutex;

    /// Generates a random string.
    static String generateEntryID()
    {
        UUID random_uuid;
        static Poco::UUIDGenerator generator;
        generator.createRandom().copyTo(reinterpret_cast<char *>(&random_uuid));
        return toString(random_uuid);
    }

    /// Converts an entry to string so it can be written to ZooKeeper.
    static String entryToString(const Entry & entry)
    {
        WriteBufferFromOwnString out;
        writeVarUInt(entry.part_infos.size(), out);
        for (const auto & part_info : entry.part_infos)
            part_info.serialize(out);
        writeVarUInt(entry.mutation_names.size(), out);
        for (const auto & mutation_name : entry.mutation_names)
            writeBinary(mutation_name, out);
        return out.str();
    }

    /// Converts an entry from string after reading it from ZooKeeper.
    static Entry parseEntryFromString(const String & str)
    {
        ReadBufferFromString in{str};
        Entry entry;
        size_t num_part_infos;
        readVarUInt(num_part_infos, in);
        for (size_t i = 0; i != num_part_infos; ++i)
        {
            MergeTreePartInfo part_info;
            part_info.deserialize(in);
            entry.part_infos.emplace(std::move(part_info));
        }
        size_t num_mutations;
        readVarUInt(num_mutations, in);
        for (size_t i = 0; i != num_mutations; ++i)
        {
            String mutation_name;
            readBinary(mutation_name, in);
            entry.mutation_names.insert(mutation_name);
        }
        return entry;
    }

    /// Splits a long string into parts.
    /// We have to do that because the data of a node in ZooKeeper can't be longer than 1 MB by default).
    static Strings splitIntoParts(const String & str, size_t max_part_size)
    {
        if (!max_part_size)
            return {str};
        size_t num_parts = (str.length() + max_part_size - 1) / max_part_size;
        Strings res;
        res.reserve(num_parts);
        for (size_t i = 0; i != num_parts; ++i)
            res.emplace_back(str.substr(i * max_part_size, max_part_size));
        return res;
    }

    /// Makes zookeeper paths to store a specified entry in ZooKeeper.
    Strings getSerializationPaths(const String & entry_id, size_t count) const
    {
        SerializationPathFormatting formatting;
        formatting.replica_name = storage.replica_name;
        formatting.entry_id = entry_id;
        formatting.count = count;

        Strings res;
        res.reserve(count);
        for (size_t i = 0; i != count; ++i)
        {
            formatting.index = i + 1;
            res.emplace_back(fs::path{serialization_root_path} / formatSerializationPath(formatting));
        }
        return res;
    }

    /// Writes an entry to ZooKeeper.
    void saveEntryToZookeeper(const Strings & serialization_paths, const Strings & entry_as_string_parts, const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        LOG_INFO(log, "Saving info about currently restoring parts to ZooKeeper to {}", boost::algorithm::join(serialization_paths, ", "));

        try
        {
            chassert(serialization_paths.size() == entry_as_string_parts.size());
            std::vector<zkutil::ZooKeeper::FutureMulti> futures;

            for (size_t i = 0; i != serialization_paths.size(); ++i)
            {
                Coordination::Requests ops;

                ops.push_back(zkutil::makeCreateRequest(serialization_paths[i], entry_as_string_parts[i], zkutil::CreateMode::Persistent));

                /// Check that table is not being dropped ("host" is the first node that is removed on replica drop)
                ops.push_back(zkutil::makeCheckRequest(fs::path(storage.replica_path) / "host", -1));

                futures.emplace_back(zookeeper->asyncMulti(ops));
            }

            for (auto & future : futures)
                future.get();
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to save info about currently restoring parts to ZooKeeper to {}", boost::algorithm::join(serialization_paths, ", "));
            throw;
        }
    }

    /// Reads an entry to ZooKeeper.
    std::optional<Entry> loadEntryFromZookeeper(const Strings & serialization_paths, const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        LOG_INFO(log, "Loading info about currently restoring parts from ZooKeeper from {}", boost::algorithm::join(serialization_paths, ", "));

        auto responses = zookeeper->tryGet(serialization_paths);

        String entry_as_string;

        for (size_t i = 0; i != responses.size(); ++i)
        {
            const Coordination::GetResponse & response = responses[i];
            if (response.error == Coordination::Error::ZNONODE)
                return {}; /// That's ok, probably other replica has already removed that node.

            entry_as_string += response.data;
        }

        return parseEntryFromString(entry_as_string);
    }

    /// Tries to remove an entry from ZooKeeper.
    void tryRemoveEntryFromZookeeper(const Strings & serialization_paths, const ZooKeeperWithFaultInjectionPtr & zookeeper) const
    {
        LOG_INFO(log, "Removing info about currently restoring parts from ZooKeeper from {}", boost::algorithm::join(serialization_paths, ", "));
        TemporaryZookeeperNodes temp_nodes{zookeeper};
        temp_nodes.addPaths(serialization_paths);
    }

    struct UpdateInfo
    {
        String replica_name;
        Strings serialization_paths;
        bool complete = false; /// Whether `serialization_paths` contains all serialization paths of a corresponding entry?
    };

    /// Reads from ZooKeeper a list of children of the `serialization_root_path` node and parse those paths.
    std::unordered_map<String, UpdateInfo>
    readUpdateInfos(const ZooKeeperWithFaultInjectionPtr & zookeeper, Coordination::WatchCallback watch_callback) const
    {
        Coordination::Stat stat;
        Strings paths = zookeeper->getChildrenWatch(serialization_root_path, &stat, watch_callback);

        std::vector<ParsedSerializationPath> parsed_paths;
        parsed_paths.reserve(paths.size());
        for (const String & path : paths)
        {
            auto parsed_path = parseSerializationPath(path);
            if (parsed_path)
                parsed_paths.emplace_back(*parsed_path);
        }

        std::unordered_map<String, UpdateInfo> update_infos;
        Strings bad_entry_ids;

        for (const auto & parsed_path : parsed_paths)
        {
            const auto & path = parsed_path.path;
            const auto & formatting = parsed_path.formatting;
            const auto & entry_id = formatting.entry_id;

            auto it = update_infos.find(entry_id);
            bool just_added = false;
            if (it == update_infos.end())
            {
                it = update_infos.emplace(entry_id, UpdateInfo{}).first;
                just_added = true;
            }

            auto & update_info = it->second;

            if (just_added)
            {
                update_info.replica_name = formatting.replica_name;
                update_info.serialization_paths.resize(formatting.count);
            }

            if ((update_info.replica_name == formatting.replica_name) && (update_info.serialization_paths.size() == formatting.count)
                && (1 <= formatting.count) && (1 <= formatting.index) && (formatting.index <= formatting.count))
            {
                update_info.serialization_paths[formatting.index - 1] = fs::path{serialization_root_path} / path;
            }
            else
            {
                bad_entry_ids.emplace_back(entry_id);
            }
        }

        /// If some children of `serialization_root_path` have wrong format we'll just ignore them.
        for (const auto & bad_entry_id : bad_entry_ids)
            update_infos.erase(bad_entry_id);

        for (auto & [_, update_info] : update_infos)
        {
            update_info.complete = true;
            for (const auto & serialization_path : update_info.serialization_paths)
            {
                if (serialization_path.empty())
                    update_info.complete = false;
            }
        }

        return update_infos;
    }

    struct SerializationPathFormatting
    {
        String replica_name;
        String entry_id;
        size_t count = 0;
        size_t index = 0;
    };

    /// Formats a serialization zookeeper path to write an entry to ZooKeeper.
    /// The following format is used: "<entry_id>_<count>_<index>_<replica_name>"
    static String formatSerializationPath(const SerializationPathFormatting & formatting)
    {
        return fmt::format("{}_{}_{}_{}", formatting.entry_id, formatting.count, formatting.index, formatting.replica_name);
    }

    struct ParsedSerializationPath
    {
        String path;
        SerializationPathFormatting formatting;
    };

    /// Retrieves a formatting info from a serialization zookeeper path.
    static std::optional<ParsedSerializationPath> parseSerializationPath(const String & path)
    {
        /// <entry_id>_<count>_<index>_<replica_name>
        size_t separator1 = path.find('_');
        if (separator1 == String::npos)
            return {};
        size_t separator2 = path.find('_', separator1 + 1);
        if (separator2 == String::npos)
            return {};
        size_t separator3 = path.find('_', separator2 + 1);
        if (separator3 == String::npos)
            return {};

        ParsedSerializationPath res;
        res.path = path;
        auto & formatting = res.formatting;
        formatting.entry_id = path.substr(0, separator1);
        if (!tryParse(formatting.count, path.substr(separator1 + 1, separator2 - separator1 - 1)))
            return {};
        if (!tryParse(formatting.index, path.substr(separator2 + 1, separator3 - separator2 - 1)))
            return {};
        formatting.replica_name = path.substr(separator3 + 1);
        return res;
    }
};


std::optional<String> ReplicatedMergeTreeCurrentlyRestoringFromBackup::BlockNumbersAllocator::findAnyRestoringPart(const ZooKeeperWithFaultInjectionPtr & zookeeper) const
{
    currently_restoring_info.update(zookeeper);
    return currently_restoring_info.findAnyRestoringPart();
}


ReplicatedMergeTreeCurrentlyRestoringFromBackup::ReplicatedMergeTreeCurrentlyRestoringFromBackup(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log(&Poco::Logger::get(storage_.getLogName() + " (RestorerFromBackup)"))
    , currently_restoring_info(std::make_unique<CurrentlyRestoringInfo>(storage_, log))
    , block_numbers_allocator(std::make_unique<BlockNumbersAllocator>(storage_, log, *currently_restoring_info))
{
}

ReplicatedMergeTreeCurrentlyRestoringFromBackup::~ReplicatedMergeTreeCurrentlyRestoringFromBackup() = default;

bool ReplicatedMergeTreeCurrentlyRestoringFromBackup::containsPart(const MergeTreePartInfo & part_info) const
{
    return currently_restoring_info->containsPart(part_info);
}

bool ReplicatedMergeTreeCurrentlyRestoringFromBackup::containsMutation(const String & mutation_name) const
{
    return currently_restoring_info->containsMutation(mutation_name);
}

void ReplicatedMergeTreeCurrentlyRestoringFromBackup::update(const ZooKeeperWithFaultInjectionPtr & zookeeper, Coordination::WatchCallback watch_callback)
{
    currently_restoring_info->update(zookeeper, watch_callback);
}

scope_guard ReplicatedMergeTreeCurrentlyRestoringFromBackup::allocateBlockNumbers(
    std::vector<MergeTreePartInfo> & part_infos_,
    std::vector<MutationInfoFromBackup> & mutation_infos_,
    bool check_no_parts_before_,
    String & zookeeper_path_for_checking_,
    const ContextPtr & context_)
{
    auto no_parts_check_reason = NoPartsCheckReason::NONE;
    if (check_no_parts_before_)
        no_parts_check_reason = NoPartsCheckReason::NON_EMPTY_TABLE_IS_NOT_ALLOWED;
    else if (!mutation_infos_.empty())
        no_parts_check_reason = NoPartsCheckReason::RESTORING_MUTATIONS;

    auto get_zookeeper = [this] { return storage.getZooKeeper(); };
    WithRetries::KeeperSettings keeper_settings{context_};
    WithRetries with_retries{log, get_zookeeper, keeper_settings};
    auto holder = with_retries.createRetriesControlHolder("allocateBlockNumbersForReplicated");

    scope_guard remove_entry;

    holder.retries_ctl.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
    {
        /// Remove the entry from the previous retry.
        remove_entry = {};

        with_retries.renewZooKeeper(zookeeper);

        if (storage.is_readonly)
        {
            /// Stop retries if in shutdown
            if (storage.shutdown_called)
                throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode due to shutdown: replica_path={}", storage.replica_path);

            /// If we are not going to check that there are no parts exists then it's okay to be in read-only mode.
            if (no_parts_check_reason != NoPartsCheckReason::NONE)
            {
                holder.retries_ctl.setUserError(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode: replica_path={}", storage.replica_path);
                return;
            }
        }

        /// Create temporary zookeeper nodes to allocate block numbers and mutation numbers.
        auto temp_nodes = block_numbers_allocator->allocateBlockNumbers(
            part_infos_, mutation_infos_, no_parts_check_reason, zookeeper, context_);

        /// Store information about parts and mutations we're going to restore in memory and ZooKeeper.
        remove_entry = currently_restoring_info->addEntry(part_infos_, mutation_infos_, zookeeper_path_for_checking_, zookeeper, keeper_settings);

        /// Remove temporary nodes (the destructor of `temp_nodes` could do that too, but removeNodes() is better here because it checks errors).
        LOG_INFO(log, "Removing ephemeral nodes creates to allocate block numbers and mutation numbers");
        temp_nodes.removeNodes();
    });

    return remove_entry;
}

}
