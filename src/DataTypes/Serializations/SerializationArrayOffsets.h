#pragma once

#include <DataTypes/Serializations/SerializationNumber.h>

namespace DB
{

/// Class for deserialization of Array offsets as a separate subcolumn.
class SerializationArrayOffsets : public SerializationNumber<UInt64>
{
public:
    void deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const override;
};


}
