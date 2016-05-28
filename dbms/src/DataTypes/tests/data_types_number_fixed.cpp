#include <iostream>
#include <fstream>

#include <DB/Common/Stopwatch.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	std::shared_ptr<ColumnUInt64> column = std::make_shared<ColumnUInt64>();
	ColumnUInt64::Container_t & vec = column->getData();
	DataTypeUInt64 data_type;

	Stopwatch stopwatch;
	size_t n = 10000000;

	vec.resize(n);
	for (size_t i = 0; i < n; ++i)
		vec[i] = i;

	std::ofstream ostr("test");
	WriteBufferFromOStream out_buf(ostr);

	stopwatch.restart();
	data_type.serializeBinary(*column, out_buf);
	stopwatch.stop();

	std::cout << "Elapsed: " << stopwatch.elapsedSeconds() << std::endl;

	return 0;
}
