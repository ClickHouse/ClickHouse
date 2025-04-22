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
}
