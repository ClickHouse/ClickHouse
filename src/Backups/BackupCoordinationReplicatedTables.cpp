#include <Backups/BackupCoordinationReplicatedTables.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Common/Exception.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_BACKUP_TABLE;
    extern const int LOGICAL_ERROR;
}


namespace
{
    struct LessReplicaName
    {
        bool operator()(const std::shared_ptr<const String> & left, const std::shared_ptr<const String> & right) { return *left < *right; }
    };
}

using MutationInfo = IBackupCoordination::MutationInfo;


class BackupCoordinationReplicatedTables::CoveredPartsFinder
{
public:
    explicit CoveredPartsFinder(const String & table_name_for_logs_) : table_name_for_logs(table_name_for_logs_) {}

    void addPartInfo(MergeTreePartInfo && new_part_info, const std::shared_ptr<const String> & replica_name)
    {
        auto new_min_block = new_part_info.min_block;
        auto new_max_block = new_part_info.max_block;
        auto & parts = partitions[new_part_info.partition_id];

        /// Find the first part with max_block >= `part_info.min_block`.
        auto first_it = parts.lower_bound(new_min_block);
        if (first_it == parts.end())
        {
            /// All max_blocks < part_info.min_block, so we can safely add the `part_info` to the list of parts.
            parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_name});
            return;
        }

        {
            /// part_info.min_block <= current_info.max_block
            const auto & part = first_it->second;
            if (new_max_block < part.info.min_block)
            {
                /// (prev_info.max_block < part_info.min_block) AND (part_info.max_block < current_info.min_block),
                /// so we can safely add the `part_info` to the list of parts.
                parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_name});
                return;
            }

            /// (part_info.min_block <= current_info.max_block) AND (part_info.max_block >= current_info.min_block), parts intersect.

            if (part.info.contains(new_part_info))
            {
                /// `part_info` is already contained in another part.
                return;
            }
        }

        /// Probably `part_info` is going to replace multiple parts, find the range of parts to replace.
        auto last_it = first_it;
        while (last_it != parts.end())
        {
            const auto & part = last_it->second;
            if (part.info.min_block > new_max_block)
                break;
            if (!new_part_info.contains(part.info))
            {
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Intersected parts detected: {} on replica {} and {} on replica {}",
                    part.info.getPartName(),
                    *part.replica_name,
                    new_part_info.getPartName(),
                    *replica_name);
            }
            ++last_it;
        }

        /// `part_info` will replace multiple parts [first_it..last_it)
        parts.erase(first_it, last_it);
        parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_name});
    }

    bool isCoveredByAnotherPart(const String & part_name) const
    {
        return isCoveredByAnotherPart(MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING));
    }

    bool isCoveredByAnotherPart(const MergeTreePartInfo & part_info) const
    {
        auto partition_it = partitions.find(part_info.partition_id);
        if (partition_it == partitions.end())
            return false;

        const auto & parts = partition_it->second;

        /// Find the first part with max_block >= `part_info.min_block`.
        auto it_part = parts.lower_bound(part_info.min_block);
        if (it_part == parts.end())
        {
            /// All max_blocks < part_info.min_block, so there is no parts covering `part_info`.
            return false;
        }

        /// part_info.min_block <= current_info.max_block
        const auto & existing_part = it_part->second;
        if (part_info.max_block < existing_part.info.min_block)
        {
            /// (prev_info.max_block < part_info.min_block) AND (part_info.max_block < current_info.min_block),
            /// so there is no parts covering `part_info`.
            return false;
        }

        /// (part_info.min_block <= current_info.max_block) AND (part_info.max_block >= current_info.min_block), parts intersect.

        if (existing_part.info == part_info)
        {
            /// It's the same part, it's kind of covers itself, but we check in this function whether a part is covered by another part.
            return false;
        }

        /// Check if `part_info` is covered by `current_info`.
        return existing_part.info.contains(part_info);
    }

private:
    struct PartInfo
    {
        MergeTreePartInfo info;
        std::shared_ptr<const String> replica_name;
    };

    using Parts = std::map<Int64 /* max_block */, PartInfo>;
    std::unordered_map<String, Parts> partitions;
    const String table_name_for_logs;
};


BackupCoordinationReplicatedTables::BackupCoordinationReplicatedTables() = default;
BackupCoordinationReplicatedTables::~BackupCoordinationReplicatedTables() = default;

void BackupCoordinationReplicatedTables::addPartNames(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addPartNames() must not be called after preparing");

    auto & table_info = table_infos[table_shared_id];
    table_info.table_name_for_logs = table_name_for_logs;

    if (!table_info.covered_parts_finder)
        table_info.covered_parts_finder = std::make_unique<CoveredPartsFinder>(table_name_for_logs);

    auto replica_name_ptr = std::make_shared<String>(replica_name);

    for (const auto & part_name_and_checksum : part_names_and_checksums)
    {
        const auto & part_name = part_name_and_checksum.part_name;
        const auto & checksum = part_name_and_checksum.checksum;
        auto it = table_info.replicas_by_part_name.find(part_name);
        if (it == table_info.replicas_by_part_name.end())
        {
            it = table_info.replicas_by_part_name.emplace(part_name, PartReplicas{}).first;
            it->second.checksum = checksum;
        }
        else
        {
            const auto & other = it->second;
            if (other.checksum != checksum)
            {
                const String & other_replica_name = **other.replica_names.begin();
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Table {} on replica {} has part {} which is different from the part on replica {}. Must be the same",
                    table_name_for_logs,
                    replica_name,
                    part_name,
                    other_replica_name);
            }
        }

        auto & replica_names = it->second.replica_names;

        /// `replica_names` should be ordered because we need this vector to be in the same order on every replica.
        replica_names.insert(
            std::upper_bound(replica_names.begin(), replica_names.end(), replica_name_ptr, LessReplicaName{}), replica_name_ptr);
    }
}

Strings BackupCoordinationReplicatedTables::getPartNames(const String & table_shared_id, const String & replica_name) const
{
    prepare();

    auto it = table_infos.find(table_shared_id);
    if (it == table_infos.end())
        return {};

    const auto & part_names_by_replica_name = it->second.part_names_by_replica_name;
    auto it2 = part_names_by_replica_name.find(replica_name);
    if (it2 == part_names_by_replica_name.end())
        return {};

    return it2->second;
}

void BackupCoordinationReplicatedTables::addMutations(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<MutationInfo> & mutations)
{
    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addMutations() must not be called after preparing");

    auto & table_info = table_infos[table_shared_id];
    table_info.table_name_for_logs = table_name_for_logs;
    for (const auto & [mutation_id, mutation_entry] : mutations)
        table_info.mutations.emplace(mutation_id, mutation_entry);

    /// std::max() because the calculation must give the same result being repeated on a different replica.
    table_info.replica_name_to_store_mutations = std::max(table_info.replica_name_to_store_mutations, replica_name);
}

std::vector<MutationInfo>
BackupCoordinationReplicatedTables::getMutations(const String & table_shared_id, const String & replica_name) const
{
    prepare();

    auto it = table_infos.find(table_shared_id);
    if (it == table_infos.end())
        return {};

    const auto & table_info = it->second;
    if (table_info.replica_name_to_store_mutations != replica_name)
        return {};

    std::vector<MutationInfo> res;
    for (const auto & [mutation_id, mutation_entry] : table_info.mutations)
        res.emplace_back(MutationInfo{mutation_id, mutation_entry});
    return res;
}

void BackupCoordinationReplicatedTables::addDataPath(const String & table_shared_id, const String & data_path)
{
    auto & table_info = table_infos[table_shared_id];
    table_info.data_paths.emplace(data_path);
}

Strings BackupCoordinationReplicatedTables::getDataPaths(const String & table_shared_id) const
{
    auto it = table_infos.find(table_shared_id);
    if (it == table_infos.end())
        return {};

    const auto & table_info = it->second;
    return Strings{table_info.data_paths.begin(), table_info.data_paths.end()};
}


void BackupCoordinationReplicatedTables::prepare() const
{
    if (prepared)
        return;

    size_t counter = 0;
    for (const auto & table_info : table_infos | boost::adaptors::map_values)
    {
        try
        {
            /// Remove parts covered by other parts.
            for (const auto & [part_name, part_replicas] : table_info.replicas_by_part_name)
            {
                auto part_info = MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);

                auto & min_data_versions_by_partition = table_info.min_data_versions_by_partition;
                auto it2 = min_data_versions_by_partition.find(part_info.partition_id);
                if (it2 == min_data_versions_by_partition.end())
                    min_data_versions_by_partition[part_info.partition_id] = part_info.getDataVersion();
                else
                    it2->second = std::min(it2->second, part_info.getDataVersion());

                table_info.covered_parts_finder->addPartInfo(std::move(part_info), part_replicas.replica_names[0]);
            }

            for (const auto & [part_name, part_replicas] : table_info.replicas_by_part_name)
            {
                if (table_info.covered_parts_finder->isCoveredByAnotherPart(part_name))
                    continue;
                size_t chosen_index = (counter++) % part_replicas.replica_names.size();
                const auto & chosen_replica_name = *part_replicas.replica_names[chosen_index];
                table_info.part_names_by_replica_name[chosen_replica_name].push_back(part_name);
            }

            /// Remove finished or unrelated mutations.
            std::unordered_map<String, String> unfinished_mutations;
            for (const auto & [mutation_id, mutation_entry_str] : table_info.mutations)
            {
                auto mutation_entry = ReplicatedMergeTreeMutationEntry::parse(mutation_entry_str, mutation_id);
                std::map<String, Int64> new_block_numbers;
                for (const auto & [partition_id, block_number] : mutation_entry.block_numbers)
                {
                    auto it = table_info.min_data_versions_by_partition.find(partition_id);
                    if ((it != table_info.min_data_versions_by_partition.end()) && (it->second < block_number))
                        new_block_numbers[partition_id] = block_number;
                }
                mutation_entry.block_numbers = std::move(new_block_numbers);
                if (!mutation_entry.block_numbers.empty())
                    unfinished_mutations[mutation_id] = mutation_entry.toString();
            }
            table_info.mutations = unfinished_mutations;
        }
        catch (Exception & e)
        {
            e.addMessage("While checking data of table {}", table_info.table_name_for_logs);
            throw;
        }
    }

    prepared = true;
}

}
