#pragma once
#include "Common/ZooKeeper/IKeeper.h"
#include "Disks/ObjectStorages/StoredObject.h"
#include "IO/ReadHelpers.h"
#include "VFSTraits.h"

namespace DB
{
using VFSObsoleteObjects = StoredObjects;

// TODO myrrc we know that for s3 remote path size is exactly 41 chars (see S3ObjectStorage.cpp)
// and references probably won't exceed a char -- maybe we could fit object info in one 64 bit
// stack-allocated field
struct VFSLogItem : std::map<String /* remote_path */, int /*references */>
{
    void merge(VFSLogItem && other);
    static String getSerialised(StoredObjects && link, StoredObjects && unlink);
};

template <>
VFSLogItem parseFromString<VFSLogItem>(std::string_view str);
}

template <>
struct fmt::formatter<DB::VFSLogItem>
{
    constexpr auto parse(auto & ctx) { return ctx.begin(); }
    constexpr auto format(const DB::VFSLogItem & item, auto & ctx)
    {
        fmt::format_to(ctx.out(), "VFSLogItem(\n");
        for (const auto & [path, links] : item)
            fmt::format_to(ctx.out(), "Item({}, links={})\n", path, links);
        return fmt::format_to(ctx.out(), ")");
    }
};
