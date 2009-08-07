#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/Exception.h>
#include <Poco/NumberFormatter.h>

#include <DB/CompressedOutputStream.h>
#include <DB/CompressedInputStream.h>


int main(int argc, char ** argv)
{
	try
	{
		int n = 1024 * 1024 * 10;
		std::string str = "123456789";
		Poco::Stopwatch stopwatch;

		std::ofstream out_f("test");
		DB::CompressedOutputStream compressor(out_f);
		
		{
			stopwatch.restart();

			for (int i = 0; i < n; ++i)
				compressor << str << std::endl;

			compressor.close();
			out_f.close();

			stopwatch.stop();
			std::cout << "Compressing: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
		}

		std::ifstream in_f("test");
		DB::CompressedInputStream decompressor(in_f);

		int i = 0;
		std::string read_str;

		{
			stopwatch.restart();

			while (std::getline(decompressor, read_str))
			{
				++i;
				/*if (read_str != str)
					throw Poco::Exception(read_str);*/
			}

			stopwatch.stop();
			std::cout << "Decompressing: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
		}
		
		if (i != n)
			throw Poco::Exception(Poco::NumberFormatter::format(i));
	}
	catch (Poco::Exception & e)
	{
		std::cerr << e.message() << std::endl;
		return 1;
	}
	
	return 0;
}
