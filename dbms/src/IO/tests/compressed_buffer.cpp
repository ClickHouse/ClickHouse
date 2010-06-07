#include <string>

#include <iostream>
#include <fstream>

#include <DB/Core/Types.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/CompressedInputStream.h>
#include <DB/IO/CompressedOutputStream.h>


int main(int argc, char ** argv)
{
	try
	{
		size_t n = 10000000;
	
		{
			std::ofstream ostr("test1");
			DB::WriteBufferFromOStream buf(ostr);
			DB::CompressedWriteBuffer compressed_buf(buf);

			for (size_t i = 0; i < n; ++i)
				DB::writeIntText(i, compressed_buf);
		}

		{
			std::ofstream ostr("test2");
			DB::CompressedOutputStream compressed_ostr(ostr);
			DB::WriteBufferFromOStream compressed_buf(compressed_ostr);

			for (size_t i = 0; i < n; ++i)
				DB::writeIntText(i, compressed_buf);
		}

		std::cerr << "Writing done." << std::endl;

		/*{
			std::ifstream istr("test1");
			DB::ReadBufferFromIStream buf(istr);
			DB::CompressedReadBuffer compressed_buf(buf);
			std::string s;

			for (size_t i = 0; i < n; ++i)
			{
				size_t x;
				DB::readIntText(x, compressed_buf);
				if (x != i)
					throw DB::Exception("Failed!");
			}
		}*/

		{
			std::ifstream istr("test2");
			DB::CompressedInputStream compressed_istr(istr);
			DB::ReadBufferFromIStream compressed_buf(compressed_istr);
			std::string s;

			for (size_t i = 0; i < n; ++i)
			{
				size_t x;
				DB::readIntText(x, compressed_buf);
				if (x != i)
					throw DB::Exception("Failed!");
			}
		}

		std::cerr << "Reading done." << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
