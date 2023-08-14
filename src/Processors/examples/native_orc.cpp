#include <string>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>

using namespace DB;

int main()
{
    ReadBufferFromFile in("/data1/clickhouse_official/data/user_files/bigolive_audience_stats_orc.orc");
    ORCSchemaReader schema_reader(in, {});
    auto schema = schema_reader.readSchema();
    std::cout << "schema:" << schema.toString() << std::endl;
    return 0;
}
