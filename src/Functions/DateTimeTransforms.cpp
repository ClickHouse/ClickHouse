#include <Functions/DateTimeTransforms.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

void throwDateIsNotSupported(const char * name)
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type Date of argument for function {}", name);
}

void throwDateTimeIsNotSupported(const char * name)
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
}

void throwDate32IsNotSupported(const char * name)
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type Date32 of argument for function {}", name);
}

}
