#include <iostream>
#include <iomanip>

#include <Poco/Stopwatch.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnString.h>

#include <DB/DataStreams/OneBlockInputStream.h>

#include <DB/Interpreters/Aggregator.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		size_t n = argc == 2 ? atoi(argv[1]) : 10;

		Block block;

		ColumnWithTypeAndName column_x;
		column_x.name = "x";
		column_x.type = new DataTypeInt16;
		ColumnInt16 * x = new ColumnInt16;
		column_x.column = x;
		auto & vec_x = x->getData();

		vec_x.resize(n);
		for (size_t i = 0; i < n; ++i)
			vec_x[i] = i % 9;

		block.insert(column_x);

		const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

		ColumnWithTypeAndName column_s1;
		column_s1.name = "s1";
		column_s1.type = new DataTypeString;
		column_s1.column = new ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s1.column->insert(std::string(strings[i % 5]));

		block.insert(column_s1);

		ColumnWithTypeAndName column_s2;
		column_s2.name = "s2";
		column_s2.type = new DataTypeString;
		column_s2.column = new ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s2.column->insert(std::string(strings[i % 3]));

		block.insert(column_s2);

		BlockInputStreamPtr stream = new OneBlockInputStream(block);
		AggregatedDataVariants aggregated_data_variants;

		Names key_column_names;
		key_column_names.emplace_back("x");
		key_column_names.emplace_back("s1");

		AggregateFunctionFactory factory;

		AggregateDescriptions aggregate_descriptions(1);

		DataTypes empty_list_of_types;
		aggregate_descriptions[0].function = factory.get("count", empty_list_of_types);

		Aggregator::Params params(key_column_names, aggregate_descriptions, false);
		Aggregator aggregator(params);

		{
			Poco::Stopwatch stopwatch;
			stopwatch.start();

			aggregator.execute(stream, aggregated_data_variants);

			stopwatch.stop();
			std::cout << std::fixed << std::setprecision(2)
				<< "Elapsed " << stopwatch.elapsed() / 1000000.0 << " sec."
				<< ", " << n * 1000000 / stopwatch.elapsed() << " rows/sec."
				<< std::endl;
		}
	}
	catch (const Exception & e)
	{
		std::cerr << e.displayText() << std::endl;
	}

	return 0;
}
