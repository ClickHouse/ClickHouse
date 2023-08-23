#pragma once

#include <base/types.h>

#include <memory>
#include <unordered_map>

namespace DB
{

class StatusAccumulator
{
public:
    struct TableStatus
    {
        size_t all_partitions_count;
        size_t processed_partitions_count;
    };

    using Map = std::unordered_map<String, TableStatus>;
    using MapPtr = std::shared_ptr<Map>;

    static MapPtr fromJSON(String state_json);
    static String serializeToJSON(MapPtr statuses);
};

}
