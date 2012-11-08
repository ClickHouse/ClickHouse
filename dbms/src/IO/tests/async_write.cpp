#include <iostream>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/AsynchronousWriteBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/copyData.h>


int main(int argc, char ** argv)
{
	try
	{
		DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
		DB::WriteBufferFromFileDescriptor out1(STDOUT_FILENO);
		DB::AsynchronousWriteBuffer out2(out1);
		DB::CompressedWriteBuffer out3(out2);

		DB::copyData(in1, out3);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
