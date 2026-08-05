#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <base/demangle.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

void throwBadTypeidCast(const std::type_info & from, const std::type_info & to)
{
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to {}",
                        demangle(from.name()), demangle(to.name()));
}
