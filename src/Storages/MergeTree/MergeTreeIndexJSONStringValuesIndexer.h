#pragma once

#include <Common/PODArray.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ITokenizer.h>

namespace DB
{

class ColumnDynamic;
class ColumnObject;
class DataTypeObject;
class IColumn;
struct MergeTreeIndexTextGranuleBuilder;

/// Walks String leaves of a `ColumnObject` slice and emits path-scoped tokens for `jsonStringValues`.
class JSONStringValuesIndexer
{
public:
    explicit JSONStringValuesIndexer(MergeTreeIndexTextGranuleBuilder & granule_builder_);

    void addRow(const ColumnObject & column_object, const DataTypeObject & type_object, size_t row);

private:
    void emitString(std::string_view path, std::string_view value);
    void processValue(std::string_view path, const IColumn & column, const DataTypePtr & type, size_t row);
    void processObject(std::string_view prefix, const ColumnObject & column_object, const DataTypeObject & type_object, size_t row);
    void processDynamic(std::string_view path, const ColumnDynamic & column_dynamic, size_t row);
    void processSharedDataValue(std::string_view path, std::string_view value_data);

    MergeTreeIndexTextGranuleBuilder & granule_builder;
    SplitByNonAlphaTokenizer split;
    PaddedPODArray<UInt8> token;
    UInt32 token_position = 0;
    UnorderedMapWithMemoryTracking<String, SerializationPtr> shared_serializations_cache;
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> shared_columns_cache;
};

}
