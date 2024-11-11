#include <memory>
#include <IO/ParallelReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Storages/ObjectStorage/HDFS/AsynchronousReadBufferFromHDFS.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <base/types.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/threadPoolCallbackRunner.h>

using namespace DB;

int main(int /*argc*/, char ** argv)
{
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    auto shared_context = SharedContextHolder(Context::createShared());
    auto global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setConfig(config);

    getIOThreadPool().initialize(100, 0, 100);


    setenv("LIBHDFS3_CONF", "/path/to/config", true); /// NOLINT
    String hdfs_uri = "hdfs://clustername";
    String hdfs_file_path = "/path/to/file";
    ReadSettings read_settings;

    auto get_read_buffer = [&](bool async, bool prefetch, bool read_at) -> ReadBufferPtr
    {
        auto rb = std::make_shared<ReadBufferFromHDFS>(hdfs_uri, hdfs_file_path, *config, read_settings, 0, true);

        if (async)
        {
            std::cout << "use async" << std::endl;
            if (prefetch)
                std::cout << "use prefetch" << std::endl;
            if (read_at)
                std::cout << "use read_at" << std::endl;

            read_settings.remote_fs_prefetch = prefetch;
            return std::make_shared<AsynchronousReadBufferFromHDFS>(
                getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER), read_settings, rb, read_at);
        }
        else
        {
            return rb;
        }
    };

    auto wrap_parallel_if_needed = [&](ReadBufferPtr input, bool parallel) -> ReadBufferPtr
    {
        if (parallel)
        {
            std::cout << "use parallel" << std::endl;
            return wrapInParallelReadBufferIfSupported(
                *input, threadPoolCallbackRunnerUnsafe<void>(getIOThreadPool().get(), "ParallelRead"), 4, 10 * 1024 * 1024, 1529522144);
        }
        else
            return input;
    };

    bool async = (std::atoi(argv[1]) != 0);
    bool prefetch = (std::atoi(argv[2]) != 0);
    bool read_at = (std::atoi(argv[3]) != 0);
    bool parallel = (std::atoi(argv[4]) != 0);
    std::cout << "async: " << async << ", prefetch: " << prefetch << ", read_at: " << read_at << ", parallel: " << parallel << std::endl;
    auto holder = get_read_buffer(async, prefetch, read_at);
    auto rb = wrap_parallel_if_needed(holder, parallel);

    String download_path = "./download";
    WriteBufferFromFile write_buffer(download_path);
    copyData(*rb, write_buffer);
    return 0;
}
