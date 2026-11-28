#include <IO/VarInt.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int INCORRECT_DATA;
}

void throwReadAfterEOF()
{
    throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after eof");
}

void throwVarUIntOutOfRange(UInt64 value, UInt64 max_value)
{
    throw Exception(
        ErrorCodes::INCORRECT_DATA,
        "VarUInt value {} is out of range for target type with maximum value {}",
        value,
        max_value);
}

void throwVarIntOutOfRange(Int64 value, Int64 min_value, Int64 max_value)
{
    throw Exception(
        ErrorCodes::INCORRECT_DATA,
        "VarInt value {} is out of range for target type [{}, {}]",
        value,
        min_value,
        max_value);
}

}
