#include <iostream>

/** Tests launcher for ClickHouse.
  * The tool walks through given or default folder in order to find files with
  * tests' description and launches it.
  */

namespace DB
{

namespace ErrorCodes
{
	extern const int POCO_EXCEPTION;
	extern const int STD_EXCEPTION;
	extern const int UNKNOWN_EXCEPTION;
}

class PerformanceTest
{
public:
	PerformanceTest()
	{
		// std::cerr << std::fixed << std::setprecision(3);
	}

private:
};

}

int mainEntryClickhousePerformanceTest(int argc, char ** argv) {
    return 0;
}
