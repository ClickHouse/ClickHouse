#pragma once

#include <Storages/MutationCommands.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>


namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class IMergeTreeDataPartInfoForReader;

/// Alter conversions which should be applied on-fly for part.
/// Built from of the most recent mutation commands for part.
/// Now ALTER RENAME COLUMN, ALTER UPDATE and ALTER DELETE are applied.
class AlterConversions : private WithContext, boost::noncopyable
{
public:
    AlterConversions() = default;

    AlterConversions(StorageMetadataPtr metadata_snapshot_, ContextPtr context_)
        : WithContext(context_)
        , metadata_snapshot(std::move(metadata_snapshot_))
    {
    }

    struct RenamePair
    {
        std::string rename_to;
        std::string rename_from;
    };

    void addMutationCommand(const MutationCommand & command);
    const std::vector<RenamePair> & getRenameMap() const { return rename_map; }

    /// Column was renamed (lookup by value in rename_map)
    bool columnHasNewName(const std::string & old_name) const;
    /// Get new name for column (lookup by value in rename_map)
    std::string getColumnNewName(const std::string & old_name) const;
    /// Is this name is new name of column (lookup by key in rename_map)
    bool isColumnRenamed(const std::string & new_name) const;
    /// Get column old name before rename (lookup by key in rename_map)
    std::string getColumnOldName(const std::string & new_name) const;

    static bool isSupportedDataMutation(MutationCommand::Type type);
    static bool isSupportedMetadataMutation(MutationCommand::Type type);

    const NameSet & getAllUpdatedColumns() const { return all_updated_columns; }
    bool hasMutations() const { return !mutation_commands.empty(); }

    /// Returns prewhere expression steps to apply
    /// mutations that affect columns from @read_columns.
    PrewhereExprSteps getMutationSteps(
        const IMergeTreeDataPartInfoForReader & part_info,
        const NamesAndTypesList & read_columns) const;

private:
    /// Returns a chain of actions that can be
    /// applied to block to execute mutation commands
    /// that affect columns from @read_columns.
    std::vector<MutationActions> getMutationActions(
        const IMergeTreeDataPartInfoForReader & part_info,
        const NamesAndTypesList & read_columns,
        bool can_execute) const;

    /// Adds source columns of expressions of MATERIALIZED columns from @read_columns if any.
    void addColumnsRequiredForMaterialized(Names & read_columns, NameSet & read_columns_set) const;

    /// Returns only mutations commands that affect columns from set.
    MutationCommands filterMutationCommands(Names & read_columns, NameSet read_columns_set) const;

    StorageMetadataPtr metadata_snapshot;

    /// Rename map new_name -> old_name.
    std::vector<RenamePair> rename_map;

    /// All mutations commands that should be applied.
    MutationCommands mutation_commands;

    /// The position of first ALTER MODIFY COLUMN command.
    std::optional<size_t> position_of_alter_conversion;

    /// The number of ALTER MODIFY COLUMN commands.
    /// Applying on-fly mutations is not supported
    /// in presence of more than one of such commands.
    size_t number_of_alter_conversions = 0;

    /// Names of columns which are updated by mutation commands.
    NameSet all_updated_columns;
};

using AlterConversionsPtr = std::shared_ptr<const AlterConversions>;

}
