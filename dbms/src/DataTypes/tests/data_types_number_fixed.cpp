#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


int main(int argc, char ** argv)
{
	Poco::SharedPtr<DB::ColumnUInt64> column = new DB::ColumnUInt64();
	DB::ColumnUInt64::Container_t & vec = column->getData();
	DB::DataTypeUInt64 data_type;

	Poco::Stopwatch stopwatch;
	size_t n = 10000000;

	vec.resize(n);
	for (size_t i = 0; i < n; ++i)
		vec[i] = i;

	std::ofstream ostr("test");
	DB::WriteBufferFromOStream out_buf(ostr);

	stopwatch.restart();
	data_type.serializeBinary(*column, out_buf);
	stopwatch.stop();

	std::cout << "Elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;

	return 0;
}
