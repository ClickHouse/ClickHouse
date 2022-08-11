#pragma once

#include <Core/Names.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/Chunk.h>

namespace DB
{

/// Interface for entities with key-value semantics.
class IKeyValueEntity
{
public:
    virtual ~IKeyValueEntity() = default;

    /** Get primary key name that supports key-value requests.
      * Primary key can constist of multiple columns.
      */
    virtual Names getPrimaryKey() const = 0;

    /*
     * Get columns from storage using keys columns.
     *
     * @param attribute_names - if we don't need all attributes, implementation can use it to benefit from reading a subset of them.
     * If attribute_names are empty, return all attributes.
     * @param key_columns - keys to get data for. Key can be compound and represented by several columns.
     * @param found_keys_map - output parameter indicating which keys were not found. 1 - key found. 0 - key is not found.
     *
     * @return - chunk of data corresponding for keys.
     *   Number of rows in chunk is equal to size of columns in keys.
     *   If the key was not found row would have a default value.
     */
    virtual Block getColumns(const Names & attribute_names, const ColumnsWithTypeAndName & key_columns, PaddedPODArray<UInt8> & found_keys_map) const = 0;

};

}
