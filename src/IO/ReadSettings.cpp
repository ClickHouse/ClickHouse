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
    read_settings.local_fs_settings.method = LocalFSReadMethod::read;
    read_settings.remote_fs_settings.method = RemoteFSReadMethod::threadpool;
    read_settings.remote_fs_settings.prefetch = false;
    read_settings.enable_filesystem_cache = false;
    read_settings.read_through_distributed_cache = false;
    read_settings.local_fs_settings.buffer_size = METADATA_FILE_BUFFER_SIZE;
    read_settings.remote_fs_settings.buffer_size = METADATA_FILE_BUFFER_SIZE;

    return read_settings;
}

ReadSettings ReadSettings::adjustBufferSize(size_t file_size) const
{
    ReadSettings res = *this;
    res.local_fs_settings.buffer_size = std::min(std::max(1ul, file_size), local_fs_settings.buffer_size);
    res.remote_fs_settings.buffer_size = std::min(std::max(1ul, file_size), remote_fs_settings.buffer_size);
    /// Note, we do not touch prefetch_buffer_size since in case of filesystem_cache prefer_bigger_buffer_size
    /// the buffer may exceed the limit up to prefetch_buffer_size, but it will not exceed total file size.
    return res;
}

void ReadSettings::disableCachesAndLogging()
{
    enable_filesystem_cache = false;
    read_through_distributed_cache = false;
    use_page_cache_for_disks_without_file_cache = false;
    use_page_cache_with_distributed_cache = false;
    use_page_cache_for_local_disks = false;
    use_page_cache_for_object_storage = false;
    page_cache_settings.cache.reset();
    filesystem_cache_settings.enable_log = false;
    enable_filesystem_read_prefetches_log = false;
}

void ReadSettings::useForSmallRemoteRead(size_t buffer_size)
{
    remote_fs_settings.method = RemoteFSReadMethod::threadpool;
    remote_fs_settings.prefetch = false;
    remote_fs_settings.buffer_size = buffer_size;
}

}
