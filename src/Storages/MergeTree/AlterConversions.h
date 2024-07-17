#pragma once

#include <Storages/MutationCommands.h>
#include <string>
#include <unordered_map>


namespace DB
{


/// Alter conversions which should be applied on-fly for part.
/// Built from of the most recent mutation commands for part.
/// Now only ALTER RENAME COLUMN is applied.
class AlterConversions : private boost::noncopyable
{
public:
    AlterConversions() = default;

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

    static bool supportsMutationCommandType(MutationCommand::Type);

private:
    /// Rename map new_name -> old_name.
    std::vector<RenamePair> rename_map;
};

using AlterConversionsPtr = std::shared_ptr<const AlterConversions>;

}
