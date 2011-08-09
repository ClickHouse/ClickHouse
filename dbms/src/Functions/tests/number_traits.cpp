#include <iostream>

#include <DB/Functions/NumberTraits.h>


void printType(DB::UInt8 x)		{ std::cout << "UInt8" << std::endl; }
void printType(DB::UInt16 x)	{ std::cout << "UInt16" << std::endl; }
void printType(DB::UInt32 x)	{ std::cout << "UInt32" << std::endl; }
void printType(DB::UInt64 x)	{ std::cout << "UInt64" << std::endl; }
void printType(DB::Int8 x)		{ std::cout << "Int8" << std::endl; }
void printType(DB::Int16 x)		{ std::cout << "Int16" << std::endl; }
void printType(DB::Int32 x)		{ std::cout << "Int32" << std::endl; }
void printType(DB::Int64 x)		{ std::cout << "Int64" << std::endl; }
void printType(DB::Float32 x)	{ std::cout << "Float32" << std::endl; }
void printType(DB::Float64 x)	{ std::cout << "Float64" << std::endl; }


int main(int argc, char ** argv)
{
	printType(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::UInt8>::Type());
	printType(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Int32>::Type());
	printType(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Float32>::Type());
	printType(DB::NumberTraits::ResultOfSubstraction<DB::UInt8, DB::UInt8>::Type());
	printType(DB::NumberTraits::ResultOfSubstraction<DB::UInt16, DB::UInt8>::Type());
	printType(DB::NumberTraits::ResultOfSubstraction<DB::UInt16, DB::Int8>::Type());
	printType(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt16, DB::Int16>::Type());
	printType(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt32, DB::Int16>::Type());
	printType(DB::NumberTraits::ResultOfIntegerDivision<DB::UInt8, DB::Int16>::Type());
	printType(DB::NumberTraits::ResultOfModulo<DB::UInt32, DB::Int8>::Type());
	
	return 0;
}
