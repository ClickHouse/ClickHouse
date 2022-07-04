#pragma once

#include <Storages/IStorage.h>
#include <Processors/Chunk.h>

namespace DB
{


/// Storage that support key-value requests
class IKeyValueStorage : public IStorage
{
public:
    using IStorage::IStorage;

    /// Get key name that supports key-value requests
    virtual const String & getPrimaryKey() const = 0;

    /// Get data directly by keys
    virtual Chunk getByKeys(
        const ColumnWithTypeAndName & col,
        const Block & sample_block,
        PaddedPODArray<UInt8> * null_map) const = 0;
};

}
