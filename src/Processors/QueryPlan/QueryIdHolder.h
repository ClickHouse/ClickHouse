#pragma once

#include <string>

#include <boost/noncopyable.hpp>

namespace DB
{

class MergeTreeData;

/// Holds the current query id and do something meaningful in destructor.
/// Currently it's used for cleaning query id in the MergeTreeData query set.
struct QueryIdHolder : private boost::noncopyable
{
    QueryIdHolder(const std::string & query_id_, const MergeTreeData & data_);

    ~QueryIdHolder();

    std::string query_id;
    const MergeTreeData & data;
};

}
