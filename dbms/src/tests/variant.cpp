#include <iostream>
#include <boost/variant.hpp>
#include <DB/Field.h>


int main(int argc, char ** argv)
{
	DB::Field f1(DB::UInt(10));
	DB::Field f2(DB::String("test"));
	
	std::cout << (f1 < f1) << std::endl;
	std::cout << (f1 < f2) << std::endl;
	std::cout << (f1 == f1) << std::endl;
	std::cout << (f1 == f2) << std::endl;
	
	return 0;
}
