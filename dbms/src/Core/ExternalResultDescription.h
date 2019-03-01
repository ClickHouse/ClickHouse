#pragma once

#include <vector>
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
        UUID,
    };

    Block sample_block;
    std::vector<std::pair<ValueType, bool /* is_nullable */>> types;

    void init(const Block & sample_block_);
};

}
