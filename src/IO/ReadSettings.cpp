#include <IO/ReadSettings.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_READ_METHOD;
    extern const int INVALID_SETTING_VALUE;
}

ReadSettings::ReadSettings(const Context & context)
{
    const auto & settings = context.getSettingsRef();

    std::string_view read_method_str = settings.local_filesystem_read_method.value;

    if (auto opt_method = magic_enum::enum_cast<LocalFSReadMethod>(read_method_str))
        local_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for local filesystem", read_method_str);

    read_method_str = settings.remote_filesystem_read_method.value;

    if (auto opt_method = magic_enum::enum_cast<RemoteFSReadMethod>(read_method_str))
        remote_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for remote filesystem", read_method_str);

    local_fs_prefetch = settings.local_filesystem_read_prefetch;
    remote_fs_prefetch = settings.remote_filesystem_read_prefetch;

    load_marks_asynchronously = settings.load_marks_asynchronously;

    enable_filesystem_read_prefetches_log = settings.enable_filesystem_read_prefetches_log;

    remote_fs_read_max_backoff_ms = settings.remote_fs_read_max_backoff_ms;
    remote_fs_read_backoff_max_tries = settings.remote_fs_read_backoff_max_tries;
    enable_filesystem_cache = settings.enable_filesystem_cache;
    read_from_filesystem_cache_if_exists_otherwise_bypass_cache = settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache;
    enable_filesystem_cache_log = settings.enable_filesystem_cache_log;
    filesystem_cache_segments_batch_size = settings.filesystem_cache_segments_batch_size;
    filesystem_cache_reserve_space_wait_lock_timeout_milliseconds = settings.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds;

    filesystem_cache_max_download_size = settings.filesystem_cache_max_download_size;
    skip_download_if_exceeds_query_cache = settings.skip_download_if_exceeds_query_cache;

    page_cache = context.getPageCache();
    use_page_cache_for_disks_without_file_cache = settings.use_page_cache_for_disks_without_file_cache;
    read_from_page_cache_if_exists_otherwise_bypass_cache = settings.read_from_page_cache_if_exists_otherwise_bypass_cache;
    page_cache_inject_eviction = settings.page_cache_inject_eviction;

    remote_read_min_bytes_for_seek = settings.remote_read_min_bytes_for_seek;

    /// Zero read buffer will not make progress.
    if (!settings.max_read_buffer_size)
    {
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid value '{}' for max_read_buffer_size", settings.max_read_buffer_size);
    }

    local_fs_buffer_size
        = settings.max_read_buffer_size_local_fs ? settings.max_read_buffer_size_local_fs : settings.max_read_buffer_size;
    remote_fs_buffer_size
        = settings.max_read_buffer_size_remote_fs ? settings.max_read_buffer_size_remote_fs : settings.max_read_buffer_size;
    prefetch_buffer_size = settings.prefetch_buffer_size;
    direct_io_threshold = settings.min_bytes_to_use_direct_io;
    mmap_threshold = settings.min_bytes_to_use_mmap_io;
    priority = Priority{settings.read_priority};

    remote_throttler = context.getRemoteReadThrottler();
    local_throttler = context.getLocalReadThrottler();

    http_max_tries = settings.http_max_tries;
    http_retry_initial_backoff_ms = settings.http_retry_initial_backoff_ms;
    http_retry_max_backoff_ms = settings.http_retry_max_backoff_ms;
    http_skip_not_found_url_for_globs = settings.http_skip_not_found_url_for_globs;
    http_make_head_request = settings.http_make_head_request;

    mmap_cache = context.getMMappedFileCache().get();
}

ReadSettings getReadSettings()
{
    auto query_context = CurrentThread::getQueryContext();
    if (query_context)
        return query_context->getReadSettings();

    auto global_context = Context::getGlobalContextInstance();
    if (global_context)
        return global_context->getReadSettings();

    return {};
}

}
