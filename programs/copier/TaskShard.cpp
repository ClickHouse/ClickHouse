#include "TaskShard.h"

#include "TaskTable.h"

namespace DB
{

TaskShard::TaskShard(TaskTable & parent, const Cluster::ShardInfo & info_)
    : task_table(parent)
    , info(info_)
{
    list_of_split_tables_on_shard.assign(task_table.number_of_splits, DatabaseAndTableName());
}

UInt32 TaskShard::numberInCluster() const
{
    return info.shard_num;
}

UInt32 TaskShard::indexInCluster() const
{
    return info.shard_num - 1;
}

String DB::TaskShard::getDescription() const
{
    return fmt::format("N{} (having a replica {}, pull table {} of cluster {}",
                       numberInCluster(), getHostNameExample(), getQuotedTable(task_table.table_pull), task_table.cluster_pull_name);
}

String DB::TaskShard::getHostNameExample() const
{
    const auto & replicas = task_table.cluster_pull->getShardsAddresses().at(indexInCluster());
    return replicas.at(0).readableString();
}

}
