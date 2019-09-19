#pragma once

namespace DB
{

enum class JSONBDataMark
{
    Nothing = 0,
    Bool,
    Int64,
    UInt64,
    Float64,
    String,
    Object,
    Array,
    Null,
    BinaryJSON,
};

}


