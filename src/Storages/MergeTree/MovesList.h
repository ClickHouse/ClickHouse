#pragma once
#include <Storages/MergeTree/BackgroundProcessList.h>
#include <Interpreters/StorageID.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Poco/URI.h>
#include <boost/noncopyable.hpp>

namespace CurrentMetrics
{
    extern const Metric Move;
}

namespace DB
{

struct MoveInfo
{
    std::string database;
    std::string table;
    std::string partition_id;

    std::string result_part_name;
    std::string result_part_path;
    UInt64 part_size;

    Float64 elapsed;
    UInt64 thread_id;
};

struct MovesListElement : private boost::noncopyable
{
    const StorageID table_id;
    const std::string partition_id;

    // TODO(serxa): fill it
    std::string result_part_name;
    std::string result_part_path;
    UInt64 part_size{};

    Stopwatch watch;
    const UInt64 thread_id;

    MovesListElement(
        const StorageID & table_id_,
        const std::string & partition_id_);

    MoveInfo getInfo() const;
};


/// List of currently processing moves
class MovesList final : public BackgroundProcessList<MovesListElement, MoveInfo>
{
private:
    using Parent = BackgroundProcessList<MovesListElement, MoveInfo>;

public:
    MovesList()
        : Parent(CurrentMetrics::Move)
    {}
};

}
