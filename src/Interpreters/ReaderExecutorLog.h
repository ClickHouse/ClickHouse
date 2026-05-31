#pragma once

#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

/// One row per `ReaderExecutor` instance, written at destruction. Lets you
/// query per-reader-per-query cache behavior, allocations and time
/// breakdown from `system.reader_executor_log` instead of grepping the
/// debug log.
struct ReaderExecutorLogElement
{
    time_t event_time{};

    String query_id;
    String source_file_path;
    /// `nullopt` when the underlying object had `StoredObject::UnknownSize`
    /// (e.g. S3 HEAD without `Content-Length`); the executor cannot report
    /// a meaningful logical file size in that case.
    std::optional<UInt64> total_size;

    UInt64 bytes_from_page_cache = 0;
    UInt64 bytes_from_filesystem_cache = 0;
    UInt64 bytes_from_source = 0;
    UInt64 bytes_pushed_to_cache_sync = 0;
    UInt64 bytes_pushed_to_cache_async = 0;

    UInt64 cache_get_requests = 0;
    UInt64 cache_populate_requests = 0;
    UInt64 source_requests = 0;

    UInt64 cache_get_us = 0;
    UInt64 cache_populate_us = 0;
    UInt64 source_read_us = 0;
    UInt64 decrypt_us = 0;
    UInt64 prefetch_wait_us = 0;
    UInt64 sync_read_us = 0;

    UInt64 prefetch_hits = 0;
    UInt64 prefetch_cancelled = 0;
    UInt64 prefetch_pool_full = 0;
    UInt64 prefetch_discarded_running = 0;
    UInt64 prefetch_discard_wait_us = 0;
    UInt64 prefetch_issued_source_bytes = 0;
    UInt64 prefetch_issued_cache_bytes = 0;
    UInt64 prefetch_wasted_source_bytes = 0;
    UInt64 prefetch_wasted_cache_bytes = 0;

    static std::string name() { return "ReaderExecutorLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class ReaderExecutorLog : public SystemLog<ReaderExecutorLogElement>
{
    using SystemLog<ReaderExecutorLogElement>::SystemLog;
};

}
