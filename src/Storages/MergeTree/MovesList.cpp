#include <Storages/MergeTree/MovesList.h>
#include <Common/CurrentMetrics.h>
#include <base/getThreadId.h>

namespace DB
{

MovesListElement::MovesListElement(
    const StorageID & table_id_, const std::string & partition_id_)
    : table_id(table_id_)
    , partition_id(partition_id_)
    , thread_id(getThreadId())
{
}

MoveInfo MovesListElement::getInfo() const
{
    MoveInfo res;
    res.database = table_id.database_name;
    res.table = table_id.table_name;
    res.partition_id = partition_id;
    res.result_part_name = result_part_name;
    res.result_part_path = result_part_path;
    res.part_size = part_size;
    res.elapsed = watch.elapsedSeconds();
    res.thread_id = thread_id;
    return res;
}

}
