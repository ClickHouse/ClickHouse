#include <Common/ErrorCodes.h>
#include <Core/ServerSettings.h>
#include <Disks/IO/getThreadPoolReader.h>
#include <IO/AsynchronousReader.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>
#include <IO/SynchronousReader.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ThreadPoolReader.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ServerSetting
{
    extern const ServerSettingsNonZeroUInt64 threadpool_remote_fs_reader_pool_size;
    extern const ServerSettingsUInt64 threadpool_remote_fs_reader_queue_size;
    extern const ServerSettingsNonZeroUInt64 threadpool_local_fs_reader_pool_size;
    extern const ServerSettingsUInt64 threadpool_local_fs_reader_queue_size;
}

IAsynchronousReader & getThreadPoolReader(FilesystemReaderType type)
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");
    return context->getThreadPoolReader(type);
}

std::unique_ptr<IAsynchronousReader> createThreadPoolReader(
    FilesystemReaderType type, const ServerSettings & server_settings)
{
    switch (type)
    {
        case FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
        {
            auto pool_size = server_settings[ServerSetting::threadpool_remote_fs_reader_pool_size];
            auto queue_size = server_settings[ServerSetting::threadpool_remote_fs_reader_queue_size];
            return std::make_unique<ThreadPoolRemoteFSReader>(pool_size, queue_size);
        }
        case FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
        {
            auto pool_size = server_settings[ServerSetting::threadpool_local_fs_reader_pool_size];
            auto queue_size = server_settings[ServerSetting::threadpool_local_fs_reader_queue_size];
            return std::make_unique<ThreadPoolReader>(pool_size, queue_size);
        }
        case FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
        {
            return std::make_unique<SynchronousReader>();
        }
    }
}

}
