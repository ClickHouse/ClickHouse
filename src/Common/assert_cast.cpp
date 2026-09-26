#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <base/demangle.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

void throwBadAssertCast(const std::type_info & from, const std::type_info & to)
{
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to {}",
                        demangle(from.name()), demangle(to.name()));
}

void throwBadAssertCastFromException(const std::exception & e)
{
    throw DB::Exception::createDeprecated(e.what(), DB::ErrorCodes::LOGICAL_ERROR);
}
