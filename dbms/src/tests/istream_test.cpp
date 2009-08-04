#include <iostream>
#include <sstream>
#include <fstream>
#include <Poco/Types.h>
#include <Poco/BinaryWriter.h>

int main(int argc, char ** argv)
{
/*	std::stringstream s;
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
	std::cout << x << std::endl;*/
	
	std::ofstream f("test");
	Poco::BinaryWriter w(f);
	
	for (int i = 0; i < 1048576; ++i)
		w << rand() % 128;

	return 0;
}
