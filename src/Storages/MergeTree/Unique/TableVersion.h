#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteIntText.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>

#include <unordered_set>

namespace DB
{

struct TableVersion
{
    UInt64 version = 0;
    static constexpr auto format = "V1";

    std::unordered_map<MergeTreePartInfo, UInt64, MergeTreePartInfoHash> part_versions;
    TableVersion() = default;
    TableVersion(const TableVersion & table_version) = default;
    explicit TableVersion(UInt64 version_) : version(version_) { }

    UInt64 getPartVersion(MergeTreePartInfo part_info) const;

    void serialize(const String & version_path, DiskPtr disk) const;

    void deserialize(const String & version_path, DiskPtr disk);

    String dump() const;
};

using TableVersionPtr = std::unique_ptr<TableVersion>;
using SharedTableVersionPtr = std::shared_ptr<TableVersion>;
}
