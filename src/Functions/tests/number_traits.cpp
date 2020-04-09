#include <iostream>

#include <DataTypes/NumberTraits.h>


void printType(DB::UInt8) { std::cout << "UInt8"; }
void printType(DB::UInt16) { std::cout << "UInt16"; }
void printType(DB::UInt32) { std::cout << "UInt32"; }
void printType(DB::UInt64) { std::cout << "UInt64"; }
void printType(DB::Int8) { std::cout << "Int8"; }
void printType(DB::Int16) { std::cout << "Int16"; }
void printType(DB::Int32) { std::cout << "Int32"; }
void printType(DB::Int64) { std::cout << "Int64"; }
void printType(DB::Float32) { std::cout << "Float32"; }
void printType(DB::Float64) { std::cout << "Float64"; }
void printType(DB::NumberTraits::Error) { std::cout << "Error"; }

template <typename T0, typename T1>
void ifRightType()
{
    printType(T0());
    std::cout << ", ";
    printType(T1());
    std::cout << " -> ";
    printType(typename DB::NumberTraits::ResultOfIf<T0, T1>::Type());
    std::cout << std::endl;
}

template <typename T0>
void ifLeftType()
{
    ifRightType<T0, DB::UInt8>();
    ifRightType<T0, DB::UInt16>();
    ifRightType<T0, DB::UInt32>();
    ifRightType<T0, DB::UInt64>();
    ifRightType<T0, DB::Int8>();
    ifRightType<T0, DB::Int16>();
    ifRightType<T0, DB::Int32>();
    ifRightType<T0, DB::Int64>();
    ifRightType<T0, DB::Float32>();
    ifRightType<T0, DB::Float64>();
}

int main(int, char **)
{
    printType(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::UInt8>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Int32>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Float32>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfSubtraction<DB::UInt8, DB::UInt8>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfSubtraction<DB::UInt16, DB::UInt8>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfSubtraction<DB::UInt16, DB::Int8>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt16, DB::Int16>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt32, DB::Int16>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfIntegerDivision<DB::UInt8, DB::Int16>::Type()); std::cout << std::endl;
    printType(DB::NumberTraits::ResultOfModulo<DB::UInt32, DB::Int8>::Type()); std::cout << std::endl;

    ifLeftType<DB::UInt8>();
    ifLeftType<DB::UInt16>();
    ifLeftType<DB::UInt32>();
    ifLeftType<DB::UInt64>();
    ifLeftType<DB::Int8>();
    ifLeftType<DB::Int16>();
    ifLeftType<DB::Int32>();
    ifLeftType<DB::Int64>();
    ifLeftType<DB::Float32>();
    ifLeftType<DB::Float64>();

    return 0;
}
