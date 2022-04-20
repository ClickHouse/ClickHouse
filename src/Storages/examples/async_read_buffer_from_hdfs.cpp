#include <memory>
#include <string>

#include <IO/ReadBufferFromString.h>
#include <IO/SnappyReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/Hive/StorageHive.h>
#include <Common/Config/ConfigProcessor.h>
#include <Storages/HDFS/AsynchronousReadBufferFromHDFS.h>


int main()
{
    using namespace DB;
    String config_path = "/data1/clickhouse_official/conf/config.xml";
    ConfigProcessor config_processor(config_path, false, true);
    config_processor.setConfigPath(fs::path(config_path).parent_path());
    auto loaded_config = config_processor.loadConfig(false);
    auto * config = loaded_config.configuration.duplicate();

    String hdfs_namenode_url = "hdfs://testcluster/";
    String path = "/data/hive/default.db/test_text/day=2021-09-18/000000_0";
    auto in = std::make_unique<ReadBufferFromHDFS>(hdfs_namenode_url, path, *config);
    auto reader = StorageHive::getThreadPoolReader();
    AsynchronousReadBufferFromHDFS buf(reader, {}, std::move(in));

    String output;
    WriteBufferFromString out(output);
    copyData(buf, out);
    std::cout << "output:" << output << std::endl;
    return 0;
}
