#include <Storages/MergeTree/AlterConversions.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

}
