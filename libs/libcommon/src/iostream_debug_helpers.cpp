
#include "common/iostream_debug_helpers.h"

#include <exception>

std::ostream & operator<<(std::ostream & stream, const std::exception & what)
{
    stream << "exception{" << what.what() << "}";
    return stream;
}
