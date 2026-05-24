#include <IO/ReadSettings.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace
{
constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
}
namespace DB
{

ReadSettings getReadSettings()
{
    auto query_context = CurrentThread::tryGetQueryContext();
    if (query_context)
        return query_context->getReadSettings();

    auto global_context = Context::getGlobalContextInstance();
    if (global_context)
        return global_context->getReadSettings();

    return {};
}

ReadSettings getReadSettingsForMetadata()
{
    ReadSettings read_settings = getReadSettings();
    read_settings.local_fs_method = LocalFSReadMethod::read;
    read_settings.remote_fs_method = RemoteFSReadMethod::threadpool;
    read_settings.remote_fs_prefetch = false;
    read_settings.enable_filesystem_cache = false;
    read_settings.read_through_distributed_cache = false;
    read_settings.local_fs_buffer_size = METADATA_FILE_BUFFER_SIZE;
    read_settings.remote_fs_buffer_size = METADATA_FILE_BUFFER_SIZE;

    return read_settings;
}

ReadSettings ReadSettings::adjustBufferSize(size_t file_size) const
{
    ReadSettings res = *this;
    res.local_fs_buffer_size = std::min(std::max(1ul, file_size), local_fs_buffer_size);
    res.remote_fs_buffer_size = std::min(std::max(1ul, file_size), remote_fs_buffer_size);
    /// Note, we do not touch prefetch_buffer_size since in case of filesystem_cache_prefer_bigger_buffer_size
    /// the buffer may exceed the limit up to prefetch_buffer_size, but it will not exceed total file size.
    return res;
}

FilesystemCacheSettings ReadSettings::getFilesystemCacheSettings() const
{
    return FilesystemCacheSettings{
        .read_from_filesystem_cache_if_exists_otherwise_bypass_cache = read_from_filesystem_cache_if_exists_otherwise_bypass_cache,
        .filesystem_cache_segments_batch_size = filesystem_cache_segments_batch_size,
        .filesystem_cache_boundary_alignment = filesystem_cache_boundary_alignment,
        .filesystem_cache_allow_background_download = filesystem_cache_allow_background_download,
        .filesystem_cache_reserve_space_wait_lock_timeout_milliseconds = filesystem_cache_reserve_space_wait_lock_timeout_milliseconds,
        .filesystem_cache_max_download_size = filesystem_cache_max_download_size,
        .filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit = filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit,
        .enable_filesystem_cache_log = enable_filesystem_cache_log,
    };
}

PageCacheSettings ReadSettings::getPageCacheSettings() const
{
    return PageCacheSettings{
        .read_from_page_cache_if_exists_otherwise_bypass_cache = read_from_page_cache_if_exists_otherwise_bypass_cache,
        .page_cache_inject_eviction = page_cache_inject_eviction,
        .page_cache_block_size = page_cache_block_size,
        .page_cache_lookahead_blocks = page_cache_lookahead_blocks,
        .page_cache_max_coalesced_bytes = page_cache_max_coalesced_bytes,
    };
}

}
