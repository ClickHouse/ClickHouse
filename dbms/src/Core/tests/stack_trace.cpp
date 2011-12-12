#include <iostream>

#include <DB/Core/StackTrace.h>


int main(int argc, char ** argv)
{
	DB::StackTrace trace;
	std::cerr << trace.toString();

	return 0;
}
