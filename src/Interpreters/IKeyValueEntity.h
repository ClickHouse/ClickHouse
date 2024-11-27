#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <Processors/Chunk.h>
#include <Core/ColumnsWithTypeAndName.h>

namespace DB
{

/// Interface for entities with key-value semantics.
class IKeyValueEntity
{
public:
    IKeyValueEntity() = default;
    virtual ~IKeyValueEntity() = default;

    /// Get primary key name that supports key-value requests.
    /// Primary key can constist of multiple columns.
    virtual Names getPrimaryKey() const = 0;

    /*
     * Get data from storage directly by keys.
     *
     * @param keys - keys to get data for. Key can be compound and represented by several columns.
     * @param out_null_map - output parameter indicating which keys were not found.
     * @param required_columns - if we don't need all attributes, implementation can use it to benefit from reading a subset of them.
     *
     * @return - chunk of data corresponding for keys.
     *   Number of rows in chunk is equal to size of columns in keys.
     *   If the key was not found row would have a default value.
     */
    virtual Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & out_null_map, const Names & required_columns) const = 0;

    /// Header for getByKeys result
    virtual Block getSampleBlock(const Names & required_columns) const = 0;

protected:
    /// Names of result columns. If empty then all columns are required.
    Names key_value_result_names;
};

}
