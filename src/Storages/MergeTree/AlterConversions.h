#pragma once

#include <string>
#include <unordered_map>


namespace DB
{

/// Alter conversions which should be applied on-fly for part. Build from of
/// the most recent mutation commands for part. Now we have only rename_map
/// here (from ALTER_RENAME) command, because for all other type of alters
/// we can deduce conversions for part from difference between
/// part->getColumns() and storage->getColumns().
struct AlterConversions
{
    struct RenamePair
    {
        std::string rename_to;
        std::string rename_from;
    };
    /// Rename map new_name -> old_name
    std::vector<RenamePair> rename_map;

    /// Column was renamed (lookup by value in rename_map)
    bool columnHasNewName(const std::string & old_name) const;
    /// Get new name for column (lookup by value in rename_map)
    std::string getColumnNewName(const std::string & old_name) const;
    /// Is this name is new name of column (lookup by key in rename_map)
    bool isColumnRenamed(const std::string & new_name) const;
    /// Get column old name before rename (lookup by key in rename_map)
    std::string getColumnOldName(const std::string & new_name) const;
};

}
