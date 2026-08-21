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
    auto query_context = CurrentThread::getQueryContext();
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

ReadSettings ReadSettings::withNestedBuffer(bool seekable) const
{
    ReadSettings res = *this;
    if (!seekable)
        res.remote_read_buffer_restrict_seek = true;
    res.remote_read_buffer_use_external_buffer = true;
    return res;
}

}
