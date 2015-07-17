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
	try
	{
		size_t n = argc == 2 ? atoi(argv[1]) : 10;

		DB::Block block;

		DB::ColumnWithTypeAndName column_x;
		column_x.name = "x";
		column_x.type = new DB::DataTypeInt16;
		DB::ColumnInt16 * x = new DB::ColumnInt16;
		column_x.column = x;
		auto & vec_x = x->getData();

		vec_x.resize(n);
		for (size_t i = 0; i < n; ++i)
			vec_x[i] = i % 9;

		block.insert(column_x);

		const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

		DB::ColumnWithTypeAndName column_s1;
		column_s1.name = "s1";
		column_s1.type = new DB::DataTypeString;
		column_s1.column = new DB::ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s1.column->insert(std::string(strings[i % 5]));

		block.insert(column_s1);

		DB::ColumnWithTypeAndName column_s2;
		column_s2.name = "s2";
		column_s2.type = new DB::DataTypeString;
		column_s2.column = new DB::ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s2.column->insert(std::string(strings[i % 3]));

		block.insert(column_s2);

		DB::BlockInputStreamPtr stream = new DB::OneBlockInputStream(block);
		DB::AggregatedDataVariants aggregated_data_variants;

		DB::Names key_column_names;
		key_column_names.emplace_back("x");
		key_column_names.emplace_back("s1");

		DB::AggregateFunctionFactory factory;

		DB::AggregateDescriptions aggregate_descriptions(1);

		DB::DataTypes empty_list_of_types;
		aggregate_descriptions[0].function = factory.get("count", empty_list_of_types);

		DB::Aggregator aggregator(key_column_names, aggregate_descriptions, false, 0, DB::OverflowMode::THROW, nullptr, 0, 0);

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
	catch (const DB::Exception & e)
	{
		std::cerr << e.displayText() << std::endl;
	}

	return 0;
}
