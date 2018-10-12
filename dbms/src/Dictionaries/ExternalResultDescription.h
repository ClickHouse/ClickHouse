#pragma once

#include <Core/Block.h>


namespace DB
{

/** Common part for implementation of MySQLBlockInputStream, MongoDBBlockInputStream and others.
  */
struct ExternalResultDescription
{
    enum struct ValueType
    {
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        String,
        Date,
        DateTime,
        UUID
    };

    /// For Nullable source types, these types correspond to their nested types.
    std::vector<ValueType> types;

    /// May contain Nullable types.
    Block sample_block;

    void init(const Block & sample_block_);
};

}
