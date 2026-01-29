#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

struct BaseManifestEntry
{
    String name;
    Int32 state;
    String disk_name;
    String disk_path;
    String part_uuid;

    BaseManifestEntry() = default;
    BaseManifestEntry(const BaseManifestEntry &) = default;
    BaseManifestEntry & operator=(const BaseManifestEntry &) = default;
    virtual ~BaseManifestEntry() = default;

    virtual void writeText(WriteBuffer & out) const;
    virtual void readText(ReadBuffer & in);
    String toString() const;
};

}
