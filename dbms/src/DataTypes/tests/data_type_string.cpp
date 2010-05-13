#include <string>

#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>


int main(int argc, char ** argv)
{
	Poco::SharedPtr<DB::ColumnString> column = new DB::ColumnString();
	DB::ColumnUInt8::Container_t & data = dynamic_cast<DB::ColumnUInt8 &>(column->getData()).getData();
	DB::ColumnArray::Offsets_t & offsets = column->getOffsets();
	DB::DataTypeString data_type;

	Poco::Stopwatch stopwatch;
	size_t n = 10000000;

	const char * s = "Hello, world!";
	size_t size = strlen(s) + 1;

	data.resize(n * size);
	offsets.resize(n);
	for (size_t i = 0; i < n; ++i)
	{
		memcpy(&data[i * size], s, size);
		offsets[i] = (i + 1) * size;
	}

	std::ofstream ostr("/dev/null");

	stopwatch.restart();
	data_type.serializeBinary(*column, ostr);
	stopwatch.stop();

	std::cout << "Elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;

	return 0;
}
