#pragma once

#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <DataTypes/Serializations/SerializationNumber.h>

namespace DB
{

/// Class for deserialization of Array offsets as a separate subcolumn.
class SerializationArrayOffsets : public SerializationNumber<UInt64>
{
private:
    SerializationArrayOffsets() = default;

public:
    static SerializationPtr create()
    {
        auto ptr = std::unique_ptr<ISerialization>(new SerializationArrayOffsets());
        return SerializationObjectPool::getOrCreate(ptr->getHash(), std::move(ptr));
    }

    UInt128 getHash() const override;

    void deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const override;
};

}
