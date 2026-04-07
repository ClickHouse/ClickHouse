#pragma once
#include "config.h"

#if USE_ARROW || USE_PARQUET

#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/Chunk.h>

#include <arrow/table.h>

namespace DB
{

class Block;

class CHColumnToArrowColumn
{
public:
    struct Settings
    {
        /// Output columns with String data type as Arrow::String type.
        /// By default Arrow::Binary is used.
        bool output_string_as_string = false;

        /// Output columns with String data type as Arrow::FixedByteArray type.
        bool output_fixed_string_as_fixed_byte_array = true;

        /// Output LowCardinality type as Arrow::Dictionary type.
        bool low_cardinality_as_dictionary = false;
        /// Use Int types for indexes in Arrow::Dictionary instead of UInt
        bool use_signed_indexes_for_dictionary = false;
        /// Always use (U)Int64 type for indexes in Arrow::Dictionary.
        bool use_64_bit_indexes_for_dictionary = false;
    };

    CHColumnToArrowColumn(const Block & header, const std::string & format_name_, const Settings & settings_);
    CHColumnToArrowColumn(const ColumnsWithTypeAndName & header_columns_, const std::string & format_name_, const Settings & settings_);

    /// Makes a copy of this converter.
    /// This can be useful to prepare for conversion in multiple threads.
    std::unique_ptr<CHColumnToArrowColumn> clone(bool copy_arrow_schema = false) const;

    /// Initializes the arrow schema if it is not initialized yet.
    /// We can't do this in the constructor because the type of indexes for LowCardinality column can change between different chunks of data
    /// but we must use the same arrow schema for all chunks.
    /// So we use the type of indexes from the first chunk of data to generate the arrow schema
    /// because it's better than getting the type of indexes from the header where it's always UInt8).
    /// Anyway we use at least 32 bit for arrow indexes (see getArrowTypeForLowCardinalityIndexes) which should be enough for most cases.
    void initializeArrowSchema(
        const Chunk * chunk = nullptr,
        std::optional<size_t> columns_num = std::nullopt,
        const std::optional<std::unordered_map<String, Int64>> & column_to_field_id = std::nullopt);

    /// Returns the arrow schema (if it's not initialized yet this function will initialize it from the header columns).
    /// Arrow schemas are immutable (all members of class arrow::Schema are const), so
    /// it's safe to pass `std::shared_ptr<arrow::Schema>` to other threads.
    std::shared_ptr<arrow::Schema> getArrowSchema() const;

    void chChunkToArrowTable(
        std::shared_ptr<arrow::Table> & res,
        const std::vector<Chunk> & chunk,
        size_t columns_num,
        const std::optional<std::unordered_map<String, Int64>> & column_to_field_id = std::nullopt);

private:
    ColumnsWithTypeAndName header_columns;
    const std::string format_name;
    const Settings settings;

    /// We should initialize arrow schema on first call of chChunkToArrowTable, not in constructor
    /// because LowCardinality column from header always has indexes type UInt8, so, we should get
    /// proper indexes type from first chunk of data.
    std::shared_ptr<arrow::Schema> arrow_schema;

    /// Map {column name : arrow dictionary}.
    /// To avoid converting dictionary from LowCardinality to Arrow
    /// Dictionary every chunk we save it and reuse.
    std::unordered_map<std::string, MutableColumnPtr> dictionary_values;
};

}

#endif
