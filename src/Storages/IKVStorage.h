#pragma once

#include <Storages/IStorage.h>

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
    virtual FieldVector::const_iterator getByKeys(
        FieldVector::const_iterator /* begin */,
        FieldVector::const_iterator /* end */,
        const Block & /* sample_block */,
        Chunk & /* result */,
        PaddedPODArray<UInt8> * /* null_map */,
        size_t /* max_block_size */) const = 0;
};

}
