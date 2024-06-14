#include <memory>
#include <string>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <base/types.h>
#include <Common/Config/ConfigProcessor.h>

#include <Poco/Util/MapConfiguration.h>

using namespace DB;

int main()
{
    setenv("LIBHDFS3_CONF", "/path/to/hdfs-site.xml", true); /// NOLINT
    String hdfs_uri = "hdfs://cluster_name";
    String hdfs_file_path = "/path/to/hdfs/file";
    ConfigurationPtr config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    ReadSettings read_settings;
    ReadBufferFromHDFS read_buffer(hdfs_uri, hdfs_file_path, *config, read_settings, 2097152UL, false);

    String download_path = "./download";
    WriteBufferFromFile write_buffer(download_path);
    copyData(read_buffer, write_buffer);
    return 0;
}
