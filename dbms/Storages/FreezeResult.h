#include <utility>

#pragma once

namespace DB
{
/// Result of FREEZE TABLE ... query.
struct FreezeResult
{
    struct PartInfo
    {
        PartInfo(String partition_, String partition_id_, String part_name_, String backup_path_)
            : partition(std::move(partition_))
            , partition_id(std::move(partition_id_))
            , part_name(std::move(part_name_))
            , backup_path(std::move(backup_path_))
        {
        }

        String partition;
        String partition_id;
        String part_name;
        String backup_path;
    };

    String backup_name;
    std::vector<PartInfo> frozen_parts;
};

}
