#include <iostream>
#include <iomanip>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

#include <DataStreams/OneBlockInputStream.h>

#include <Interpreters/Aggregator.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    try
    {
        size_t n = argc == 2 ? atoi(argv[1]) : 10;

        Block block;

        {
            ColumnWithTypeAndName column;
            column.name = "x";
            column.type = std::make_shared<DataTypeInt16>();
            auto col = ColumnInt16::create();
            auto & vec_x = col->getData();

            vec_x.resize(n);
            for (size_t i = 0; i < n; ++i)
                vec_x[i] = i % 9;

            column.column = std::move(col);
            block.insert(column);
        }

        const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

        {
            ColumnWithTypeAndName column;
            column.name = "s1";
            column.type = std::make_shared<DataTypeString>();
            auto col = ColumnString::create();

            for (size_t i = 0; i < n; ++i)
                col->insert(std::string(strings[i % 5]));

            column.column = std::move(col);
            block.insert(column);
        }

        {
            ColumnWithTypeAndName column;
            column.name = "s2";
            column.type = std::make_shared<DataTypeString>();
            auto col = ColumnString::create();

            for (size_t i = 0; i < n; ++i)
                col->insert(std::string(strings[i % 3]));

            column.column = std::move(col);
            block.insert(column);
        }

        BlockInputStreamPtr stream = std::make_shared<OneBlockInputStream>(block);
        AggregatedDataVariants aggregated_data_variants;

        AggregateFunctionFactory factory;

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes empty_list_of_types;
        aggregate_descriptions[0].function = factory.get("count", empty_list_of_types);

        Aggregator::Params params(
            stream->getHeader(), {0, 1}, aggregate_descriptions,
            false, 0, OverflowMode::THROW, nullptr, 0, 0, 0, 0, false, "");

        Aggregator aggregator(params);

        {
            Stopwatch stopwatch;
            stopwatch.start();

            aggregator.execute(stream, aggregated_data_variants);

            stopwatch.stop();
            std::cout << std::fixed << std::setprecision(2)
                << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
                << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
                << std::endl;
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}
