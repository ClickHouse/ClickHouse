#pragma once
#include "config.h"

#if USE_ARROW || USE_PARQUET

#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <arrow/table.h>


namespace DB
{

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

    void chChunkToArrowTable(std::shared_ptr<arrow::Table> & res, const std::vector<Chunk> & chunk, size_t columns_num);

private:
    ColumnsWithTypeAndName header_columns;
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    const std::string format_name;
    Settings settings;

    /// Map {column name : arrow dictionary}.
    /// To avoid converting dictionary from LowCardinality to Arrow
    /// Dictionary every chunk we save it and reuse.
    std::unordered_map<std::string, MutableColumnPtr> dictionary_values;

    /// We should initialize arrow fields on first call of chChunkToArrowTable, not in constructor
    /// because LowCardinality column from header always has indexes type UInt8, so, we should get
    /// proper indexes type from first chunk of data.
    bool is_arrow_fields_initialized = false;
};

}

#endif
