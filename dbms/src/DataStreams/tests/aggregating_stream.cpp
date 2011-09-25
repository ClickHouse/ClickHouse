#include <iostream>
#include <iomanip>

#include <boost/assign/list_inserter.hpp>

#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnString.h>

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/AggregatingBlockInputStream.h>
#include <DB/DataStreams/FinalizingAggregatedBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>


class OneBlockInputStream : public DB::IBlockInputStream
{
private:
	const DB::Block & block;
	bool has_been_read;
public:
	OneBlockInputStream(const DB::Block & block_) : block(block_), has_been_read(false) {}

	DB::Block read()
	{
		if (!has_been_read)
		{
			has_been_read = true;
			return block;
		}
		else
			return DB::Block();
	}

	DB::String getName() const { return "OneBlockInputStream"; }
};


int main(int argc, char ** argv)
{
	try
	{
		size_t n = argc == 2 ? atoi(argv[1]) : 10;

		DB::Block block;
		
		DB::ColumnWithNameAndType column_x;
		column_x.name = "x";
		column_x.type = new DB::DataTypeInt16;
		DB::ColumnInt16 * x = new DB::ColumnInt16;
		column_x.column = x;
		std::vector<Int16> & vec_x = x->getData();

		vec_x.resize(n);
		for (size_t i = 0; i < n; ++i)
			vec_x[i] = i % 9;

		block.insert(column_x);

		const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

		DB::ColumnWithNameAndType column_s1;
		column_s1.name = "s1";
		column_s1.type = new DB::DataTypeString;
		column_s1.column = new DB::ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s1.column->insert(strings[i % 5]);

		block.insert(column_s1);

		DB::ColumnWithNameAndType column_s2;
		column_s2.name = "s2";
		column_s2.type = new DB::DataTypeString;
		column_s2.column = new DB::ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s2.column->insert(strings[i % 3]);

		block.insert(column_s2);

		DB::ColumnNumbers key_column_numbers;
		key_column_numbers.push_back(0);
		//key_column_numbers.push_back(1);

		DB::AggregateFunctionFactory factory;

		DB::AggregateDescriptions aggregate_descriptions(1);

		DB::DataTypes empty_list_of_types;
		aggregate_descriptions[0].function = factory.get("count", empty_list_of_types);

		Poco::SharedPtr<DB::DataTypes> result_types = new DB::DataTypes;
		boost::assign::push_back(*result_types)
			(new DB::DataTypeInt16)
		//	(new DB::DataTypeString)
			(new DB::DataTypeVarUInt)
			;

		DB::BlockInputStreamPtr stream = new OneBlockInputStream(block);
		stream = new DB::AggregatingBlockInputStream(stream, key_column_numbers, aggregate_descriptions);
		stream = new DB::FinalizingAggregatedBlockInputStream(stream);

		DB::WriteBufferFromOStream ob(std::cout);
		DB::TabSeparatedRowOutputStream out(ob, result_types);
		
		{
			Poco::Stopwatch stopwatch;
			stopwatch.start();

			DB::copyData(*stream, out);

			stopwatch.stop();
			std::cout << std::fixed << std::setprecision(2)
				<< "Elapsed " << stopwatch.elapsed() / 1000000.0 << " sec."
				<< ", " << n * 1000000 / stopwatch.elapsed() << " rows/sec."
				<< std::endl;
		}

		std::cout << std::endl;
		stream->dumpTree(std::cout);
		std::cout << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.message() << std::endl;
	}

	return 0;
}
