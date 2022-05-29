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
    virtual std::vector<String> getPrimaryKey() const = 0;

    /// Get data directly by keys
    virtual Chunk getByKeys(
        const ColumnsWithTypeAndName & cols,
        const Block & sample_block,
        PaddedPODArray<UInt8> * null_map,
        ContextPtr context) const = 0;
};

using IKVStoragePtr = std::shared_ptr<IKeyValueStorage>;

}
