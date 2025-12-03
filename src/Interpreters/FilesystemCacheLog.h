#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct FilesystemCacheLogElement
{
    enum class CacheType : uint8_t
    {
        READ_FROM_CACHE,
        READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
        READ_FROM_FS_BYPASSING_CACHE,
        WRITE_THROUGH_CACHE,
    };

    time_t event_time{};

    String query_id;
    String source_file_path;

    std::pair<size_t, size_t> file_segment_range{};
    std::pair<size_t, size_t> requested_range{};
    CacheType cache_type{};
    std::string file_segment_key{};
    size_t file_segment_offset = 0;
    size_t file_segment_size = 0;
    bool read_from_cache_attempted;
    String read_buffer_id{};
    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters = nullptr;

    static std::string name() { return "FilesystemCacheLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class FilesystemCacheLog : public SystemLog<FilesystemCacheLogElement>
{
    using SystemLog<FilesystemCacheLogElement>::SystemLog;
};

}
