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
    /// Rename map new_name -> old_name
    std::unordered_map<std::string, std::string> rename_map;

    bool isColumnRenamed(const std::string & new_name) const { return rename_map.count(new_name) > 0; }
    std::string getColumnOldName(const std::string & new_name) const { return rename_map.at(new_name); }
};

}
