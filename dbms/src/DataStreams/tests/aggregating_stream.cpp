#include <iostream>
#include <iomanip>

#include <IO/WriteBufferFromOStream.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/SummingSortedBlockInputStream.h>
#include <DataStreams/TabSeparatedRowOutputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();

    try
    {
        size_t n = argc > 1 ? atoi(argv[1]) : 10;
        bool nested = false, sum = false, composite = false, multivalue = false;
        for (int i = 2; i < argc; ++i)
        {
            if (strcmp(argv[i], "nested") == 0)
                nested = true;
            if (strcmp(argv[i], "composite") == 0)
                composite = true;
            else if (strcmp(argv[i], "sum") == 0)
                sum = true;
            else if (strcmp(argv[i], "multivalue") == 0)
                multivalue = true;
        }

        Block block;

        ColumnWithTypeAndName column_x;
        column_x.name = "x";
        column_x.type = std::make_shared<DataTypeUInt32>();
        auto x = ColumnUInt32::create();
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
        column_s1.column = ColumnString::create();

        size_t chunk = n / 4;
        for (size_t i = 0; i < n; ++i)
            column_s1.column->insert(std::string(strings[i / chunk]));

        block.insert(column_s1);

        ColumnWithTypeAndName column_s2;
        column_s2.name = "s2";
        column_s2.type = std::make_shared<DataTypeString>();
        column_s2.column = ColumnString::create();

        for (size_t i = 0; i < n; ++i)
            column_s2.column->insert(std::string(strings[i % 3]));

        block.insert(column_s2);

        Names key_column_names;
        key_column_names.emplace_back("x");

        if (nested)
        {
            ColumnWithTypeAndName column_k;
            column_k.name = "testMap.k";
            column_k.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt16>());
            column_k.column = ColumnArray::create(ColumnUInt16::create());

            for (UInt64 i = 0; i < n; ++i)
            {
                Array values = {i % 5, (i + 1) % 5, (i + 2) % 5};
                column_k.column->insert(values);
            }

            block.insert(column_k);
            key_column_names.emplace_back(column_k.name);

            ColumnWithTypeAndName column_v;
            column_v.name = "testMap.v";
            column_v.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
            column_v.column = ColumnArray::create(ColumnUInt64::create());

            for (UInt64 i = 0; i < n; ++i)
            {
                Array values = {i % 3, (i + 1) % 3, (i + 2) % 3};
                column_v.column->insert(values);
            }

            block.insert(column_v);
            key_column_names.emplace_back(column_v.name);

            if (multivalue)
            {
                ColumnWithTypeAndName column_v2;
                column_v2.name = "testMap.v2";
                column_v2.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
                column_v2.column = ColumnArray::create(ColumnUInt64::create());

                for (UInt64 i = 0; i < n; ++i)
                {
                    Array values = {(i % 3) * 10, ((i + 1) % 3) * 10, ((i + 2) % 3) * 10};
                    column_v2.column->insert(values);
                }

                block.insert(column_v2);
                key_column_names.emplace_back(column_v2.name);
            }

            if (composite)
            {
                ColumnWithTypeAndName column_kid;
                column_kid.name = "testMap.kID";
                column_kid.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
                column_kid.column = ColumnArray::create(ColumnUInt8::create());

                for (UInt64 i = 0; i < n; ++i)
                {
                    Array values = {(i % 2) * 10, ((i + 1) % 2) * 10, UInt64(0)};
                    column_kid.column->insert(values);
                }

                block.insert(column_kid);
                key_column_names.emplace_back(column_kid.name);
            }
        }

        auto & factory = AggregateFunctionFactory::instance();

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes empty_list_of_types;
        aggregate_descriptions[0].function = factory.get("count", empty_list_of_types);

        DataTypes result_types
        {
            std::make_shared<DataTypeUInt64>(),
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

        if (sum)
        {
            SortDescription sort_columns;
            sort_columns.push_back(SortColumnDescription("s1", 1, -1));
            BlockInputStreams streams = {stream, stream};
            stream = std::make_unique<SummingSortedBlockInputStream>(streams, sort_columns, key_column_names, DEFAULT_MERGE_BLOCK_SIZE);
        }
        else
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
