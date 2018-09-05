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
    data_type.serializeBinaryBulkWithMultipleStreams(*column, [&](const IDataType::SubstreamPath &){ return &out_buf; }, 0, 0, true, {});
    stopwatch.stop();

    std::cout << "Elapsed: " << stopwatch.elapsedSeconds() << std::endl;

    return 0;
}
