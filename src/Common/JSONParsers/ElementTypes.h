#pragma once

namespace DB
{
// Enum values match simdjson's for fast conversion
enum class ElementType : uint8_t
{
    ARRAY = '[',
    OBJECT = '{',
    INT64 = 'l',
    UINT64 = 'u',
    DOUBLE = 'd',
    STRING = '"',
    BOOL = 't',
    NULL_VALUE = 'n'
};
}
