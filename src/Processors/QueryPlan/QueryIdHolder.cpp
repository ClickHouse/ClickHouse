#include <Processors/QueryPlan/QueryIdHolder.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

QueryIdHolder::QueryIdHolder(const String & query_id_, const MergeTreeData & data_) : query_id(query_id_), data(data_)
{
}

QueryIdHolder::~QueryIdHolder()
{
    data.removeQueryId(query_id);
}

}
