#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/ReadHelpers.h>


int main()
{
	const char * s = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
	DB::ReadBuffer file_in(const_cast<char *>(s), 32, 0);
	DB::CompressedReadBuffer in(file_in);

	try
	{
		while (!in.eof())
			;
	}
	catch (...)
	{
		std::cerr << "Catched!\n";
	}

	return 0;
}


void f()
{
	DB::parse<UInt64>("123");
}
