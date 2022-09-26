#pragma once

#include <Core/Names.h>
#include <Storages/IStorage.h>
#include <Processors/Chunk.h>

namespace DB
{

/// Storage that support key-value requests
class IKeyValueStorage : public IStorage
{
public:
    using IStorage::IStorage;

    /// Get primary key name that supports key-value requests.
    /// Primary key can constist of multiple columns.
    virtual Names getPrimaryKey() const = 0;

    /*
     * Get data from storage directly by keys.
     *
     * @param keys - keys to get data for. Key can be compound and represented by several columns.
     * @param out_null_map - output parameter indicating which keys were not found.
     *
     * @return - chunk of data corresponding for keys.
     *   Number of rows in chunk is equal to size of columns in keys.
     *   If the key was not found row would have a default value.
     */
    virtual Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & out_null_map) const = 0;
};

}
