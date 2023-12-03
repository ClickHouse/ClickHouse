#include <string>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#include <IO/copyData.h>

using namespace DB;

int main()
{
    /// Read schema from orc file
    String path = "/path/to/orc/file";
    // String path = "/data1/clickhouse_official/data/user_files/bigolive_audience_stats_orc.orc";
    {
        ReadBufferFromFile in(path);
        NativeORCSchemaReader schema_reader(in, {});
        auto schema = schema_reader.readSchema();
        std::cout << "schema:" << schema.toString() << std::endl;
    }

    /// Read schema from string with orc data
    {
        ReadBufferFromFile in(path);

        String content;
        WriteBufferFromString out(content);

        copyData(in, out);

        content.resize(out.count());
        ReadBufferFromString in2(content);
        NativeORCSchemaReader schema_reader(in2, {});
        auto schema = schema_reader.readSchema();
        std::cout << "schema:" << schema.toString() << std::endl;
    }
    return 0;
}
