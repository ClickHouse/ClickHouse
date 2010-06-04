#include <string>

#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>


int main(int argc, char ** argv)
{
	Poco::Stopwatch stopwatch;
	size_t n = 10000000;
	const char * s = "Hello, world!";
	size_t size = strlen(s) + 1;
	DB::DataTypeString data_type;

	{
		Poco::SharedPtr<DB::ColumnString> column = new DB::ColumnString();
		DB::ColumnUInt8::Container_t & data = dynamic_cast<DB::ColumnUInt8 &>(column->getData()).getData();
		DB::ColumnArray::Offsets_t & offsets = column->getOffsets();
		
		data.resize(n * size);
		offsets.resize(n);
		for (size_t i = 0; i < n; ++i)
		{
			memcpy(&data[i * size], s, size);
			offsets[i] = (i + 1) * size;
		}

		std::ofstream ostr("test");
		DB::WriteBufferFromOStream out_buf(ostr);

		stopwatch.restart();
		data_type.serializeBinary(*column, out_buf);
		stopwatch.stop();

		std::cout << "Writing, elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		Poco::SharedPtr<DB::ColumnString> column = new DB::ColumnString();

		std::ifstream istr("test");
		DB::ReadBufferFromIStream in_buf(istr);

		stopwatch.restart();
		data_type.deserializeBinary(*column, in_buf, n);
		stopwatch.stop();

		std::cout << "Reading, elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;

		std::cout << std::endl
			<< boost::get<DB::String>((*column)[0]) << std::endl
			<< boost::get<DB::String>((*column)[n - 1]) << std::endl;
	}

	return 0;
}
