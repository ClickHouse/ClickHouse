#include <Common/ErrorCodes.h>
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

IAsynchronousReader & getThreadPoolReader(FilesystemReaderType type)
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");
    return context->getThreadPoolReader(type);
}

std::unique_ptr<IAsynchronousReader> createThreadPoolReader(
    FilesystemReaderType type, const Poco::Util::AbstractConfiguration & config)
{
    switch (type)
    {
        case FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
        {
            auto pool_size = config.getUInt(".threadpool_remote_fs_reader_pool_size", 250);
            auto queue_size = config.getUInt(".threadpool_remote_fs_reader_queue_size", 1000000);
            return std::make_unique<ThreadPoolRemoteFSReader>(pool_size, queue_size);
        }
        case FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
        {
            auto pool_size = config.getUInt(".threadpool_local_fs_reader_pool_size", 100);
            auto queue_size = config.getUInt(".threadpool_local_fs_reader_queue_size", 1000000);
            return std::make_unique<ThreadPoolReader>(pool_size, queue_size);
        }
        case FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
        {
            return std::make_unique<SynchronousReader>();
        }
    }
}

}
