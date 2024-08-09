#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <fmt/chrono.h>

namespace DB
{

struct SnapshotMetadata
{
    uint64_t metadata_version;
    String object_storage_key;
    uint64_t total_size;
    int32_t znode_version;
    bool is_initial_snaphot;

    SnapshotMetadata(
        uint64_t metadata_version_ = 0ull,
        const String & object_storage_key_ = "",
        uint64_t total_size_ = 0ull,
        int32_t znode_version_ = -1,
        bool is_initial_ = false)
        : metadata_version(metadata_version_)
        , object_storage_key(object_storage_key_)
        , total_size(total_size_)
        , znode_version(znode_version_)
        , is_initial_snaphot(is_initial_)
    {
    }

    SnapshotMetadata(
        const String & object_storage_key_,
        uint64_t metadata_version_ = 0ull,
        uint64_t total_size_ = 0ull,
        int32_t znode_version_ = -1,
        bool is_initial_ = false)
        : metadata_version(metadata_version_)
        , object_storage_key(object_storage_key_)
        , total_size(total_size_)
        , znode_version(znode_version_)
        , is_initial_snaphot(is_initial_)
    {
    }

    void update(const String & new_snapshot_key) { object_storage_key = new_snapshot_key; }

    String serialize() const { return fmt::format("{} {} {} ", metadata_version, object_storage_key, total_size); }

    static SnapshotMetadata deserialize(const String & str, int32_t znode_version)
    {
        SnapshotMetadata result;
        result.znode_version = znode_version;
        /// In case of initial snaphot, the content will be empty.
        if (str.empty())
        {
            result.is_initial_snaphot = true;
            return result;
        }
        ReadBufferFromString rb(str);

        readIntTextUnsafe(result.metadata_version, rb);
        checkChar(' ', rb);
        readStringUntilWhitespace(result.object_storage_key, rb);
        checkChar(' ', rb);
        readIntTextUnsafe(result.total_size, rb);
        result.is_initial_snaphot = false;
        return result;
    }
};
}
