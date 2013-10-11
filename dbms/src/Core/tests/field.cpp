#include <iostream>
#include <iomanip>
#include <sstream>

#include <DB/Core/Field.h>

#include <statdaemons/Stopwatch.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/DataTypes/DataTypeString.h>


int main(int argc, char ** argv)
{
	DB::FieldVisitorToString to_string;
	
	DB::Field field = DB::UInt64(0);
	std::cerr << DB::apply_visitor(to_string, field) << std::endl;

	field = std::string("Hello, world!");
	std::cerr << DB::apply_visitor(to_string, field) << std::endl;

	field = DB::Null();
	std::cerr << DB::apply_visitor(to_string, field) << std::endl;

	DB::Field field2;
	field2 = field;
	std::cerr << DB::apply_visitor(to_string, field2) << std::endl;

	DB::Array array;
	array.push_back(DB::UInt64(123));
	array.push_back(DB::Int64(-123));
	array.push_back(DB::String("Hello"));
	field = array;
	std::cerr << DB::apply_visitor(to_string, field) << std::endl;

	DB::get<DB::Array &>(field).push_back(field);
	std::cerr << DB::apply_visitor(to_string, field) << std::endl;

	std::cerr << (field < field2) << std::endl;
	std::cerr << (field2 < field) << std::endl;


	try
	{
		size_t n = argc == 2 ? DB::parse<UInt64>(argv[1]) : 10000000;

		Stopwatch watch;

		{
			DB::Array array(n);

			{
				Stopwatch watch;

				for (size_t i = 0; i < n; ++i)
					array[i] = DB::String(i % 32, '!');

				watch.stop();
				std::cerr << std::fixed << std::setprecision(2)
					<< "Set " << n << " fields (" << n * sizeof(array[0]) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
					<< n / watch.elapsedSeconds() << " elem/sec. (" << n * sizeof(array[0]) / watch.elapsedSeconds() / 1000000 << " MB/s.)"
					<< std::endl;
			}

			{
				Stopwatch watch;

				size_t sum = 0;
				for (size_t i = 0; i < n; ++i)
					sum += DB::safeGet<const DB::String &>(array[i]).size();

				watch.stop();
				std::cerr << std::fixed << std::setprecision(2)
					<< "Got " << n << " fields (" << n * sizeof(array[0]) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
					<< n / watch.elapsedSeconds() << " elem/sec. (" << n * sizeof(array[0]) / watch.elapsedSeconds() / 1000000 << " MB/s.)"
					<< std::endl;

				std::cerr << sum << std::endl;
			}

			watch.restart();
		}

		watch.stop();

		std::cerr << std::fixed << std::setprecision(2)
			<< "Destroyed " << n << " fields (" << n * sizeof(DB::Array::value_type) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
			<< n / watch.elapsedSeconds() << " elem/sec. (" << n * sizeof(DB::Array::value_type) / watch.elapsedSeconds() / 1000000 << " MB/s.)"
			<< std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}
	
	std::cerr << "sizeof(Field) = " << sizeof(DB::Field) << std::endl;
	
	return 0;
}
