#pragma once

#include <Common/PODArray_fwd.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>

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
     * @param required_columns - if we don't need all attributes, implementation can use it to benefit from reading a subset of them.
     * @param out_null_map - output parameter indicating which keys were not found.
     * @param out_offsets - output parameter for ALL join semantics. If provided (non-empty after call),
     *                      contains the cumulative count of result rows for each input key.
     *
     * @return - chunk of data corresponding for keys.
     *   If out_offsets is empty after call: Number of rows in chunk is equal to size of columns in keys.
     *                                       If the key was not found row would have a default value.
     *   If out_offsets is not empty: Number of rows can be different (based on ALL join semantics).
     */
    virtual Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets) const = 0;

    /// Header for getByKeys result
    virtual Block getSampleBlock(const Names & required_columns) const = 0;

protected:
    /// Names of result columns. If empty then all columns are required.
    Names key_value_result_names;
};

}
