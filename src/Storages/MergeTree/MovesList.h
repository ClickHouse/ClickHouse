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
    std::string part_name;
    std::string target_disk_name;
    std::string target_disk_path;
    UInt64 part_size;

    Float64 elapsed;
    UInt64 thread_id;
};

struct MovesListElement : private boost::noncopyable
{
    const StorageID table_id;
    const std::string part_name;
    const std::string target_disk_name;
    const std::string target_disk_path;
    const UInt64 part_size;

    Stopwatch watch;
    const UInt64 thread_id;

    MovesListElement(
        const StorageID & table_id_,
        const std::string & part_name_,
        const std::string & target_disk_name_,
        const std::string & target_disk_path_,
        UInt64 part_size_);

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
