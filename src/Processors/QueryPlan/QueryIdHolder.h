#pragma once

#include <string>

namespace DB
{
class MergeTreeData;

/// Holds the current query id and do something meaningful in destructor.
/// Currently it's used for cleaning query id in the MergeTreeData query set.
struct QueryIdHolder
{
    QueryIdHolder(const std::string & query_id_, const MergeTreeData & data_);

    ~QueryIdHolder();

    std::string query_id;
    const MergeTreeData & data;
};

}
