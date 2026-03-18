#include <Functions/DateTimeTransforms.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

void throwDateIsNotSupported(const char * name)
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument of type Date for function {}", name);
}

void throwDate32IsNotSupported(const char * name)
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument of type Date32 for function {}", name);
}

void throwDateTimeIsNotSupported(const char * name)
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument of type DateTime for function {}", name);
}

}
