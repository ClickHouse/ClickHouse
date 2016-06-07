#include <iostream>
#include <DB/Interpreters/Users.h>


using namespace DB;


int main(int argc, char ** argv)
{
	try
	{
		std::cerr << HostRegexpPattern(argv[1]).contains(Poco::Net::IPAddress(argv[2])) << std::endl;
	}
	catch (const Exception & e)
	{
		std::cerr << "Exception " << e.what() << ": " << e.displayText() << "\n" << e.getStackTrace().toString();
		return 1;
	}
	
	return 0;
}
