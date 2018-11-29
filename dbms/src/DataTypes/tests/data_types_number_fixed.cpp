#include <iostream>
#include <fstream>

#include <Common/Stopwatch.h>

#include <IO/WriteBufferFromOStream.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>


int main(int, char **)
{
    using namespace DB;

    auto column = ColumnUInt64::create();
    ColumnUInt64::Container & vec = column->getData();
    DataTypeUInt64 data_type;

    Stopwatch stopwatch;
    size_t n = 10000000;

    vec.resize(n);
    for (size_t i = 0; i < n; ++i)
        vec[i] = i;

    std::ofstream ostr("test");
    WriteBufferFromOStream out_buf(ostr);

    stopwatch.restart();
    IDataType::SerializeBinaryBulkSettings settings;
    settings.getter = [&](const IDataType::SubstreamPath &){ return &out_buf; };
    IDataType::SerializeBinaryBulkStatePtr state;
    data_type.serializeBinaryBulkStatePrefix(settings, state);
    data_type.serializeBinaryBulkWithMultipleStreams(*column, 0, 0, settings, state);
    data_type.serializeBinaryBulkStateSuffix(settings, state);
    stopwatch.stop();

    std::cout << "Elapsed: " << stopwatch.elapsedSeconds() << std::endl;

    return 0;
}
