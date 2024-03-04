#pragma once
#include "Core/Types_fwd.h"
#include "StoredObject.h"
#include "boost/container/flat_map.hpp"

namespace Poco
{
class Logger;
}

namespace DB
{
class ReadBuffer;
class WriteBuffer;
using VFSLogItemStorage = boost::container::flat_map<String /* remote_path */, int /*references delta */>;
class IVFSSnapshotReadStream;
class IVFSSnapshotWriteStream;

struct VFSMergeResult
{
    StoredObjects obsolete;
    VFSLogItemStorage invalid;
};

struct VFSLogItem : VFSLogItemStorage
{
    static VFSLogItem parse(std::string_view str);
    Strings serialize() const; // ZooKeeper imposes a 1MB limit for node hence StringS
    void add(const StoredObject & obj);
    void remove(const StoredObject & obj);
    void merge(VFSLogItem && other);
    VFSMergeResult mergeWithSnapshot(IVFSSnapshotReadStream & snapshot, IVFSSnapshotWriteStream & new_snapshot, Poco::Logger * log) &&;
};
}

template <>
struct fmt::formatter<DB::VFSLogItem>
{
    constexpr auto parse(auto & ctx) { return ctx.begin(); }
    constexpr auto format(const DB::VFSLogItem & item, auto & ctx)
    {
        fmt::format_to(ctx.out(), "VFSLogItem(\n");
        for (const auto & [path, links] : item)
            fmt::format_to(ctx.out(), "{} {}\n", path, links);
        return fmt::format_to(ctx.out(), ")");
    }
};
