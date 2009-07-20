#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/Timespan.h>
#include <Poco/Exception.h>

#include <DB/ColumnType.h>


int main(int argc, char ** argv)
{
	DB::FieldVector vec;
	vec.reserve(1000000);
	Poco::Stopwatch stopwatch;
	Poco::SharedPtr<DB::IColumnType> column_type = DB::ColumnTypeFactory::get("Text");

	{
		stopwatch.restart();
		for (int i = 0; i < 1000000; ++i)
		{
			vec.push_back("http://www.boost.org/doc/libs/1_39_0/doc/html/variant/tutorial.html#variant.tutorial.recursive.recursive-variant");
		}
		stopwatch.stop();
		std::cout << "Filling array: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ofstream ostr("test");
		stopwatch.restart();
		for (int i = 0; i < 1000000; ++i)
		{
			column_type->serializeBinary(vec[i], ostr);
		}
		stopwatch.stop();
		std::cout << "Serialization: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ifstream istr("test");
		stopwatch.restart();
		for (int i = 0; i < 1000000; ++i)
		{
			column_type->deserializeBinary(vec[i], istr);
		}
		stopwatch.stop();
		std::cout << "Deserialization: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ofstream ostr("test2");
		stopwatch.restart();
		for (int i = 0; i < 1000000; ++i)
		{
			column_type->serializeText(vec[i], ostr);
			ostr.put('\t');
		}
		stopwatch.stop();
		std::cout << "Serialization (text): " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ifstream istr("test2");
		stopwatch.restart();
		for (int i = 0; i < 1000000; ++i)
		{
			column_type->deserializeText(vec[i], istr);
		}
		stopwatch.stop();
		std::cout << "Deserialization (text): " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	return 0;
}
