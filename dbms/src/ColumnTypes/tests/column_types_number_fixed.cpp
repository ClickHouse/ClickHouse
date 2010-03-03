#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>

#include <DB/ColumnTypes/ColumnTypesNumberFixed.h>


int main(int argc, char ** argv)
{
	DB::Column column = DB::UInt64Column();
	DB::UInt64Column & vec = boost::get<DB::UInt64Column>(column);
	DB::ColumnTypeUInt64 column_type;

	Poco::Stopwatch stopwatch;
	size_t n = 10000000;

	vec.resize(n);
	for (size_t i = 0; i < n; ++i)
		vec[i] = i;

	std::ofstream ostr("/dev/null");

	stopwatch.restart();
	column_type.serializeBinary(column, ostr);
	stopwatch.stop();

	std::cout << "Elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;

	return 0;
}
