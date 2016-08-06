#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/Operators.h>
#include <DB/Core/StringRef.h>


/** Calculates StringRefHash from stdin. For debugging.
  */

int main(int argc, char ** argv)
{
	using namespace DB;

	ReadBufferFromFileDescriptor in(STDIN_FILENO);
	WriteBufferFromFileDescriptor out(STDOUT_FILENO);

	String s;
	readStringUntilEOF(s, in);
	out << StringRefHash()(s) << '\n';

	return 0;
}
