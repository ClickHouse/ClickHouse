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
    class CoveredPartsFinder
    {
    public:
        explicit CoveredPartsFinder(const Strings & replica_names_) : replica_names(replica_names_) { }

        void addPartInfo(MergeTreePartInfo && new_part_info, size_t replica_index)
        {
            auto new_min_block = new_part_info.min_block;
            auto new_max_block = new_part_info.max_block;
            auto & parts = partitions[new_part_info.partition_id];

            /// Find the first part with max_block >= `part_info.min_block`.
            auto first_it = parts.lower_bound(new_min_block);
            if (first_it == parts.end())
            {
                /// All max_blocks < part_info.min_block, so we can safely add the `part_info` to the list of parts.
                parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_index});
                return;
            }

            {
                /// part_info.min_block <= current_info.max_block
                const auto & part = first_it->second;
                if (new_max_block < part.info.min_block)
                {
                    /// (prev_info.max_block < part_info.min_block) AND (part_info.max_block < current_info.min_block),
                    /// so we can safely add the `part_info` to the list of parts.
                    parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_index});
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
                        part.info.getPartNameForLogs(),
                        replica_names[part.replica_index],
                        new_part_info.getPartNameForLogs(),
                        replica_names[replica_index]);
                }
                ++last_it;
            }

            /// `part_info` will replace multiple parts [first_it..last_it)
            parts.erase(first_it, last_it);
            parts.emplace(new_max_block, PartInfo{std::move(new_part_info), replica_index});
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
            size_t replica_index;
        };

        using Parts = std::map<Int64 /* max_block */, PartInfo>;
        std::unordered_map<String, Parts> partitions;
        const Strings & replica_names;
    };
}

using MutationInfo = IBackupCoordination::MutationInfo;


BackupCoordinationReplicatedTables::BackupCoordinationReplicatedTables() = default;
BackupCoordinationReplicatedTables::~BackupCoordinationReplicatedTables() = default;

void BackupCoordinationReplicatedTables::addPartNames(PartNamesForTableReplica && part_names)
{
    const auto & table_shared_id = part_names.table_shared_id;
    const auto & table_name_for_logs = part_names.table_name_for_logs;
    const auto & replica_name = part_names.replica_name;
    const auto & data_path = part_names.data_path;
    const auto & part_names_and_checksums = part_names.part_names_and_checksums;

    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addPartNames() must not be called after preparing");

    auto & table_info = table_infos[table_shared_id];
    table_info.table_name_for_logs = table_name_for_logs;

    size_t data_path_index = table_info.data_paths.size();
    if (auto it = std::find(table_info.data_paths.begin(), table_info.data_paths.end(), data_path); it != table_info.data_paths.end())
        data_path_index = it - table_info.data_paths.begin();
    else
        table_info.data_paths.emplace_back(data_path);

    size_t replica_index = table_info.replica_names.size();
    if (auto it = std::find(table_info.replica_names.begin(), table_info.replica_names.end(), replica_name); it != table_info.replica_names.end())
        replica_index = it - table_info.replica_names.begin();
    else
        table_info.replica_names.emplace_back(replica_name);

    for (const auto & part_name_and_checksum : part_names_and_checksums)
    {
        const auto & part_name = part_name_and_checksum.part_name;
        const auto & checksum = part_name_and_checksum.checksum;
        auto it = table_info.part_infos.find(part_name);
        if (it == table_info.part_infos.end())
        {
            it = table_info.part_infos.emplace(part_name, PartInfo{}).first;
            it->second.checksum = checksum;
        }
        else
        {
            const auto & other = it->second;
            if (other.checksum != checksum)
            {
                const String & other_replica_name = table_info.replica_names[other.default_replica_to_store];
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Table {} on replica {} has part {} different from the part on replica {} "
                    "(checksum '{}' on replica {} != checksum '{}' on replica {})",
                    table_name_for_logs, replica_name, part_name, other_replica_name,
                    getHexUIntLowercase(checksum), replica_name, getHexUIntLowercase(other.checksum), other_replica_name);
            }
        }

        PartInfo & part_info = it->second;

        part_info.replicas_to_store_by_data_path_index.resize(table_info.data_paths.size(), static_cast<size_t>(-1));
        part_info.replicas_to_store_by_data_path_index[data_path_index] = replica_index;

        /// `default_replica_to_store` should be the same on every replica.
        if (part_info.default_replica_to_store == static_cast<size_t>(-1))
        {
            part_info.default_replica_to_store = replica_index;
        }
        else
        {
            const String & prev_default_replica_name = table_info.replica_names[part_info.default_replica_to_store];
            if (replica_name < prev_default_replica_name)
                part_info.default_replica_to_store = replica_index;
        }
    }
}

std::vector<BackupCoordinationReplicatedTables::PartNameAndDataPath>
BackupCoordinationReplicatedTables::getPartNamesWithDataPaths(const String & table_shared_id, const String & replica_name) const
{
    prepare();

    auto it = table_infos.find(table_shared_id);
    if (it == table_infos.end())
        return {};

    const auto & table_info = it->second;
    auto it2 = table_info.part_names_and_data_paths.find(replica_name);
    if (it2 == table_info.part_names_and_data_paths.end())
        return {};

    const auto & part_names_and_data_paths = it2->second;

    std::vector<PartNameAndDataPath> res;
    res.reserve(part_names_and_data_paths.size());
    for (const auto & part_name_and_data_path : part_names_and_data_paths)
    {
        const String & part_name = *part_name_and_data_path.part_name;
        const String & data_path = table_info.data_paths[part_name_and_data_path.data_path_index];
        res.emplace_back(PartNameAndDataPath{part_name, data_path});
    }

    return res;
}

void BackupCoordinationReplicatedTables::addMutations(MutationsForTableReplica && mutations_for_table_replica)
{
    const auto & table_shared_id = mutations_for_table_replica.table_shared_id;
    const auto & table_name_for_logs = mutations_for_table_replica.table_name_for_logs;
    const auto & replica_name = mutations_for_table_replica.replica_name;
    const auto & mutations = mutations_for_table_replica.mutations;

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
    res.reserve(table_info.mutations.size());
    for (const auto & [mutation_id, mutation_entry] : table_info.mutations)
        res.emplace_back(MutationInfo{mutation_id, mutation_entry});
    return res;
}

void BackupCoordinationReplicatedTables::addDataPath(DataPathForTableReplica && data_path_for_table_replica)
{
    const auto & table_shared_id = data_path_for_table_replica.table_shared_id;
    const auto & data_path = data_path_for_table_replica.data_path;

    auto & table_info = table_infos[table_shared_id];

    if (std::find(table_info.data_paths.begin(), table_info.data_paths.end(), data_path) == table_info.data_paths.end())
        table_info.data_paths.emplace_back(data_path);
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

    for (const auto & table_info : table_infos | boost::adaptors::map_values)
    {
        try
        {
            /// Remove parts covered by other parts.
            CoveredPartsFinder covered_parts_finder{table_info.replica_names};

            for (const auto & [part_name, part_info] : table_info.part_infos)
            {
                auto merge_tree_part_info = MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
                const auto & partition_id = merge_tree_part_info.partition_id;
                const auto data_version = merge_tree_part_info.getDataVersion();

                auto & min_data_versions_by_partition = table_info.min_data_versions_by_partition;
                auto it2 = min_data_versions_by_partition.find(partition_id);
                if (it2 == min_data_versions_by_partition.end())
                    min_data_versions_by_partition[partition_id] = data_version;
                else
                    it2->second = std::min(it2->second, data_version);

                covered_parts_finder.addPartInfo(std::move(merge_tree_part_info), part_info.default_replica_to_store);
            }

            for (const auto & [part_name, part_info] : table_info.part_infos)
            {
                if (covered_parts_finder.isCoveredByAnotherPart(part_name))
                    continue;

                for (size_t data_path_index = 0; data_path_index != table_info.data_paths.size(); ++data_path_index)
                {
                    size_t replica_index = (data_path_index < part_info.replicas_to_store_by_data_path_index.size())
                        ? part_info.replicas_to_store_by_data_path_index[data_path_index]
                        : static_cast<size_t>(-1);
                    if (replica_index == static_cast<size_t>(-1))
                        replica_index = part_info.default_replica_to_store;
                    const String & replica_name = table_info.replica_names[replica_index];
                    table_info.part_names_and_data_paths[replica_name].emplace_back(PartNameAndDataPathIndex{&part_name, data_path_index});
                }
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
