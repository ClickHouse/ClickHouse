#include <iostream>
#include <iomanip>

#include <IO/WriteBufferFromOStream.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/TabSeparatedRowOutputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    try
    {
        size_t n = argc == 2 ? atoi(argv[1]) : 10;

        Block block;

        ColumnWithTypeAndName column_x;
        column_x.name = "x";
        column_x.type = std::make_shared<DataTypeInt16>();
        auto x = std::make_shared<ColumnInt16>();
        column_x.column = x;
        auto & vec_x = x->getData();

        vec_x.resize(n);
        for (size_t i = 0; i < n; ++i)
            vec_x[i] = i % 9;

        block.insert(column_x);

        const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

        ColumnWithTypeAndName column_s1;
        column_s1.name = "s1";
        column_s1.type = std::make_shared<DataTypeString>();
        column_s1.column = std::make_shared<ColumnString>();

        for (size_t i = 0; i < n; ++i)
            column_s1.column->insert(std::string(strings[i % 5]));

        block.insert(column_s1);

        ColumnWithTypeAndName column_s2;
        column_s2.name = "s2";
        column_s2.type = std::make_shared<DataTypeString>();
        column_s2.column = std::make_shared<ColumnString>();

        for (size_t i = 0; i < n; ++i)
            column_s2.column->insert(std::string(strings[i % 3]));

        block.insert(column_s2);

        Names key_column_names;
        key_column_names.emplace_back("x");

        AggregateFunctionFactory factory;

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes empty_list_of_types;
        aggregate_descriptions[0].function = factory.get("count", empty_list_of_types);

        DataTypes result_types
        {
            std::make_shared<DataTypeInt16>(),
        //    std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeUInt64>(),
        };

        Block sample;
        for (DataTypes::const_iterator it = result_types.begin(); it != result_types.end(); ++it)
        {
            ColumnWithTypeAndName col;
            col.type = *it;
            sample.insert(std::move(col));
        }

        Aggregator::Params params(key_column_names, aggregate_descriptions, false);

        BlockInputStreamPtr stream = std::make_shared<OneBlockInputStream>(block);
        stream = std::make_shared<AggregatingBlockInputStream>(stream, params, true);

        WriteBufferFromOStream ob(std::cout);
        RowOutputStreamPtr row_out = std::make_shared<TabSeparatedRowOutputStream>(ob, sample);
        BlockOutputStreamPtr out = std::make_shared<BlockOutputStreamFromRowOutputStream>(row_out);

        {
            Stopwatch stopwatch;
            stopwatch.start();

            copyData(*stream, *out);

            stopwatch.stop();
            std::cout << std::fixed << std::setprecision(2)
                << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
                << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
                << std::endl;
        }

        std::cout << std::endl;
        stream->dumpTree(std::cout);
        std::cout << std::endl;
    }
    catch (const Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}
