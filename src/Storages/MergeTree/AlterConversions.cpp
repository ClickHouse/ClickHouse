#include <Storages/MergeTree/AlterConversions.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool AlterConversions::isSupportedDataMutation(MutationCommand::Type)
{
    /// Currently there is no such mutations. See setting 'apply_mutations_on_fly'.
    return false;
}

bool AlterConversions::isSupportedMetadataMutation(MutationCommand::Type type)
{
    return type == MutationCommand::Type::RENAME_COLUMN;
}

void AlterConversions::addMutationCommand(const MutationCommand & command)
{
    /// Currently only RENAME_COLUMN is applied on-fly.
    if (command.type == MutationCommand::Type::RENAME_COLUMN)
        rename_map.emplace_back(RenamePair{command.rename_to, command.column_name});
}

bool AlterConversions::columnHasNewName(const std::string & old_name) const
{
    for (const auto & [new_name, prev_name] : rename_map)
    {
        if (old_name == prev_name)
            return true;
    }

    return false;
}

std::string AlterConversions::getColumnNewName(const std::string & old_name) const
{
    for (const auto & [new_name, prev_name] : rename_map)
    {
        if (old_name == prev_name)
            return new_name;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} was not renamed", old_name);
}

bool AlterConversions::isColumnRenamed(const std::string & new_name) const
{
    for (const auto & [name_to, name_from] : rename_map)
    {
        if (name_to == new_name)
            return true;
    }
    return false;
}

/// Get column old name before rename (lookup by key in rename_map)
std::string AlterConversions::getColumnOldName(const std::string & new_name) const
{
    for (const auto & [name_to, name_from] : rename_map)
    {
        if (name_to == new_name)
            return name_from;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} was not renamed", new_name);
}

}
