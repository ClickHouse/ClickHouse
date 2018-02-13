
#include "common/iostream_debug_helpers.h"

#include <exception>

namespace std
{

std::ostream & operator<<(std::ostream & stream, const std::exception & what)
{
    stream << "exception{" << what.what() << "}";
    return stream;
}

}
