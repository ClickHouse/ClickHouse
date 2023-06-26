#include <iostream>
#include <memory>
#include <string>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <base/types.h>
#include <Common/Config/ConfigProcessor.h>

using namespace DB;

int main()
{
    setenv("LIBHDFS3_CONF", "/data1/clickhouse_official/conf/hdfs-site.bigocluster.xml", true); /// NOLINT
    String hdfs_uri = "hdfs://bigocluster";
    String hdfs_file_path = "/data/hive/report_tb.db/bigolive_wj_pos_sdk_video_stats_event_allv1/day=2023-03-14/"
                            "part-00014-272de29e-098c-4007-987a-f6b7ae740402-c000";
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    ReadSettings read_settings;
    ReadBufferFromHDFS read_buffer(hdfs_uri, hdfs_file_path, *config, read_settings, 625150306UL, false);

    String download_path = "./download";
    WriteBufferFromFile write_buffer(download_path);
    copyData(read_buffer, write_buffer);
}
