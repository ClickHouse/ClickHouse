#pragma once

#include "config.h"

#if USE_MONGODB
#include <Common/JSONBuilder.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <Storages/StorageMongoDB.h>

#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/database.hpp>

namespace DB
{

/// Creates MongoDB connection and cursor, converts it to a stream of blocks
class MongoDBSource final : public ISource
{
public:
    MongoDBSource(
        const mongocxx::uri & uri,
        const std::string & collection_name,
        const bsoncxx::document::view_or_value & query,
        const mongocxx::options::find & options,
        const Block & sample_block_,
        const UInt64 & max_block_size_);

    ~MongoDBSource() override;

    String getName() const override { return "MongoDB"; }

private:
    MongoDBInstanceHolder & instance_holder = MongoDBInstanceHolder::instance();

    static void insertDefaultValue(IColumn & column, const IColumn & sample_column);
    void insertValue(IColumn & column, const size_t & idx, const DataTypePtr & type, const std::string & name, const bsoncxx::document::element & value);

    Chunk generate() override;

    mongocxx::client client;
    mongocxx::database database;
    mongocxx::collection collection;
    mongocxx::cursor cursor;

    Block sample_block;
    std::unordered_map<size_t, std::pair<size_t, std::pair<DataTypePtr, Field>>> arrays_info;
    const UInt64 max_block_size;

    JSONBuilder::FormatSettings json_format_settings;
    bool all_read = false;

    static JSONBuilder::FormatSettings createJSONFormatSettings()
    {
        auto settings = DB::FormatSettings{};
        settings.json = FormatSettings::JSON{
            0,  // max_depth
            false, // array_of_rows
            false,  // quote_64bit_integers [changed]
            false, // quote_64bit_floats
            false,  // quote_denormals [changed]
            false, // quote_decimals
            true,  // escape_forward_slashes
            false, // read_named_tuples_as_objects
            false, // use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects
            true,  // write_named_tuples_as_objects
            false, // skip_null_value_in_named_tuples
            false, // defaults_for_missing_elements_in_named_tuple
            false, // ignore_unknown_keys_in_named_tuple
            false, // serialize_as_strings
            true,  // read_bools_as_numbers
            true,  // read_bools_as_strings
            true,  // read_numbers_as_strings
            true,  // read_objects_as_strings
            true,  // read_arrays_as_strings
            false, // try_infer_numbers_from_strings
            true,  // validate_types_from_metadata
            false, // validate_utf8
            false, // allow_deprecated_object_type
            false, // allow_json_type
            false, // valid_output_on_exception
            false, // compact_allow_variable_number_of_columns
            false, // try_infer_objects_as_tuples
            true,  // infer_incomplete_types_as_strings
            true,  // throw_on_bad_escape_sequence
            true,  // ignore_unnecessary_fields
            false, // empty_as_default
            false, // type_json_skip_duplicated_paths
            true,  // pretty_print
            ' ',   // pretty_print_indent
            4      // pretty_print_indent_multiplier
        };
        return JSONBuilder::FormatSettings{settings, 0, true, true};
    }
};

}
#endif
