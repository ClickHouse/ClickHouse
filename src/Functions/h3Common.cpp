#include <Functions/h3Common.h>

#include <Common/Exception.h>

#if USE_H3

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void validateH3Cell(UInt64 h)
{
    if (!isValidCell(h))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid H3 cell index: {}", h);
}

}

#endif
