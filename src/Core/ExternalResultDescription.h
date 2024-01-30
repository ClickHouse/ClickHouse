#pragma once

#include <vector>
#include <Core/Block.h>


namespace DB
{
/** Common part for implementation of MySQLSource, MongoDBSource and others.
  */
struct ExternalResultDescription
{
    enum struct ValueType
    {
        vtUInt8,
        vtUInt16,
        vtUInt32,
        vtUInt64,
        vtInt8,
        vtInt16,
        vtInt32,
        vtInt64,
        vtFloat32,
        vtFloat64,
        vtEnum8,
        vtEnum16,
        vtString,
        vtDate,
        vtDate32,
        vtDateTime,
        vtUUID,
        vtDateTime64,
        vtDecimal32,
        vtDecimal64,
        vtDecimal128,
        vtDecimal256,
        vtArray,
        vtFixedString,
        vtPoint,
    };

    Block sample_block;
    std::vector<std::pair<ValueType, bool /* is_nullable */>> types;

    ExternalResultDescription() = default;
    explicit ExternalResultDescription(const Block & sample_block_);

    void init(const Block & sample_block_);
};

}
