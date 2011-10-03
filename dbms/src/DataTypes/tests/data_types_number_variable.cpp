#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>


int main(int argc, char ** argv)
{
	const char * file_name = "/dev/shm/test";
	Poco::SharedPtr<DB::ColumnUInt64> column1 = new DB::ColumnUInt64();
	Poco::SharedPtr<DB::ColumnUInt64> column2 = new DB::ColumnUInt64();
	DB::ColumnUInt64::Container_t & vec1 = column1->getData();
	DB::DataTypeVarUInt data_type;

	Poco::Stopwatch stopwatch;
	size_t n = atoi(argv[1]);

	vec1.resize(n);
	for (size_t i = 0; i < n; ++i)
		vec1[i] = i;

	{
		std::ofstream ostr(file_name);
		DB::WriteBufferFromOStream out_buf(ostr);

		stopwatch.restart();
		data_type.serializeBinary(*column1, out_buf);
		stopwatch.stop();

		std::cout << "Writing, elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ifstream istr(file_name);
		DB::ReadBufferFromIStream in_buf(istr);
	
		stopwatch.restart();
		data_type.deserializeBinary(*column2, in_buf, n);
		stopwatch.stop();

		std::cout << "Reading, elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	return 0;
}
