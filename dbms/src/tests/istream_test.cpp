#include <iostream>
#include <sstream>
#include <fstream>
#include <Poco/Types.h>
#include <Poco/BinaryWriter.h>

int main(int argc, char ** argv)
{
	std::stringstream s;
	s << "192.168.1.1fls";
	
	unsigned x;
	s >> x;
	s.get();
	std::cout << x << std::endl;
	s >> x;
	s.get();
	std::cout << x << std::endl;
	s >> x;
	s.get();
	std::cout << x << std::endl;
	s >> x;
	std::cout << x << std::endl;

	return 0;
}
