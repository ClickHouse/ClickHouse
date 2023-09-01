#include <memory>
#include <string>
#include <filesystem>

#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromString.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context.h>
#include <Common/Config/ConfigProcessor.h>
#include <Storages/HDFS/AsynchronousReadBufferFromHDFS.h>

int main()
{
    using namespace DB;
    namespace fs = std::filesystem;

    String config_path = "/path/to/config/file";
    ConfigProcessor config_processor(config_path, false, true);
    config_processor.setConfigPath(fs::path(config_path).parent_path());
    auto loaded_config = config_processor.loadConfig(false);
    auto * config = loaded_config.configuration.duplicate();

    String hdfs_namenode_url = "hdfs://namenode:port/";
    String path = "/path/to/hdfs/file";
    ReadSettings settings = {};
    auto in = std::make_unique<ReadBufferFromHDFS>(hdfs_namenode_url, path, *config, settings);
    auto reader = IObjectStorage::getThreadPoolReader();
    AsynchronousReadBufferFromHDFS buf(reader, {}, std::move(in));

    String output;
    WriteBufferFromString out(output);
    copyData(buf, out);
    std::cout << "output:" << output << std::endl;
    return 0;
}
