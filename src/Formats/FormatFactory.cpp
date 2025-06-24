#include <Formats/FormatFactory.h>

#include <unistd.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Formats/Impl/MySQLOutputFormat.h>
#include <Processors/Formats/Impl/ParallelFormattingOutputFormat.h>
#include <Processors/Formats/Impl/ParallelParsingInputFormat.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Common/KnownObjectNames.h>
#include <Common/RemoteHostFilter.h>
#include <Common/tryGetFileNameByFileDescriptor.h>
#include <Core/FormatFactorySettings.h>
#include <Core/Settings.h>

#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{
namespace Setting
{
    /// There are way too many format settings to handle extern declarations manually.
#define DECLARE_FORMAT_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) \
    extern Settings ## TYPE NAME;
FORMAT_FACTORY_SETTINGS(DECLARE_FORMAT_EXTERN, INITIALIZE_SETTING_EXTERN)
#undef DECLARE_FORMAT_EXTERN

    extern const SettingsBool allow_experimental_object_type;
    extern const SettingsBool allow_experimental_json_type;
    extern const SettingsBool http_write_exception_in_output_format;
    extern const SettingsBool input_format_parallel_parsing;
    extern const SettingsBool log_queries;
    extern const SettingsUInt64 max_download_buffer_size;
    extern const SettingsMaxThreads max_download_threads;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsMaxThreads max_parsing_threads;
    extern const SettingsUInt64 max_memory_usage;
    extern const SettingsUInt64 max_memory_usage_for_user;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsNonZeroUInt64 min_chunk_bytes_for_parallel_parsing;
    extern const SettingsBool output_format_parallel_formatting;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsInt64 zstd_window_log_max;
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 interactive_delay;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT;
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT;
    extern const int BAD_ARGUMENTS;
}

bool FormatFactory::exists(const String & name) const
{
    return dict.find(boost::to_lower_copy(name)) != dict.end();
}

const FormatFactory::Creators & FormatFactory::getCreators(const String & name) const
{
    auto it = dict.find(boost::to_lower_copy(name));
    if (dict.end() != it)
        return it->second;
    throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown format {}", name);
}

FormatFactory::Creators & FormatFactory::getOrCreateCreators(const String & name)
{
    String lower_case = boost::to_lower_copy(name);
    auto it = dict.find(lower_case);
    if (dict.end() != it)
        return it->second;

    auto & creators = dict[lower_case];
    creators.name = name;
    return creators;
}

FormatSettings getFormatSettings(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    return getFormatSettings(context, settings);
}

FormatSettings getFormatSettings(const ContextPtr & context, const Settings & settings)
{
    FormatSettings format_settings;

    format_settings.avro.allow_missing_fields = settings[Setting::input_format_avro_allow_missing_fields];
    format_settings.avro.output_codec = settings[Setting::output_format_avro_codec];
    format_settings.avro.output_sync_interval = settings[Setting::output_format_avro_sync_interval];
    format_settings.avro.schema_registry_url = settings[Setting::format_avro_schema_registry_url].toString();
    format_settings.avro.string_column_pattern = settings[Setting::output_format_avro_string_column_pattern].toString();
    format_settings.avro.output_rows_in_file = settings[Setting::output_format_avro_rows_in_file];
    format_settings.csv.allow_double_quotes = settings[Setting::format_csv_allow_double_quotes];
    format_settings.csv.allow_single_quotes = settings[Setting::format_csv_allow_single_quotes];
    format_settings.csv.serialize_tuple_into_separate_columns = settings[Setting::output_format_csv_serialize_tuple_into_separate_columns];
    format_settings.csv.deserialize_separate_columns_into_tuple = settings[Setting::input_format_csv_deserialize_separate_columns_into_tuple];
    format_settings.csv.crlf_end_of_line = settings[Setting::output_format_csv_crlf_end_of_line];
    format_settings.csv.allow_cr_end_of_line = settings[Setting::input_format_csv_allow_cr_end_of_line];
    format_settings.csv.delimiter = settings[Setting::format_csv_delimiter];
    format_settings.csv.tuple_delimiter = settings[Setting::format_csv_delimiter];
    format_settings.csv.empty_as_default = settings[Setting::input_format_csv_empty_as_default];
    format_settings.csv.enum_as_number = settings[Setting::input_format_csv_enum_as_number];
    format_settings.csv.null_representation = settings[Setting::format_csv_null_representation];
    format_settings.csv.arrays_as_nested_csv = settings[Setting::input_format_csv_arrays_as_nested_csv];
    format_settings.csv.use_best_effort_in_schema_inference = settings[Setting::input_format_csv_use_best_effort_in_schema_inference];
    format_settings.csv.skip_first_lines = settings[Setting::input_format_csv_skip_first_lines];
    format_settings.csv.try_detect_header = settings[Setting::input_format_csv_detect_header];
    format_settings.csv.skip_trailing_empty_lines = settings[Setting::input_format_csv_skip_trailing_empty_lines];
    format_settings.csv.trim_whitespaces = settings[Setting::input_format_csv_trim_whitespaces];
    format_settings.csv.allow_whitespace_or_tab_as_delimiter = settings[Setting::input_format_csv_allow_whitespace_or_tab_as_delimiter];
    format_settings.csv.allow_variable_number_of_columns = settings[Setting::input_format_csv_allow_variable_number_of_columns];
    format_settings.csv.use_default_on_bad_values = settings[Setting::input_format_csv_use_default_on_bad_values];
    format_settings.csv.try_infer_numbers_from_strings = settings[Setting::input_format_csv_try_infer_numbers_from_strings];
    format_settings.csv.try_infer_strings_from_quoted_tuples = settings[Setting::input_format_csv_try_infer_strings_from_quoted_tuples];
    format_settings.hive_text.fields_delimiter = settings[Setting::input_format_hive_text_fields_delimiter];
    format_settings.hive_text.collection_items_delimiter = settings[Setting::input_format_hive_text_collection_items_delimiter];
    format_settings.hive_text.map_keys_delimiter = settings[Setting::input_format_hive_text_map_keys_delimiter];
    format_settings.hive_text.allow_variable_number_of_columns = settings[Setting::input_format_hive_text_allow_variable_number_of_columns];
    format_settings.custom.escaping_rule = settings[Setting::format_custom_escaping_rule];
    format_settings.custom.field_delimiter = settings[Setting::format_custom_field_delimiter];
    format_settings.custom.result_after_delimiter = settings[Setting::format_custom_result_after_delimiter];
    format_settings.custom.result_before_delimiter = settings[Setting::format_custom_result_before_delimiter];
    format_settings.custom.row_after_delimiter = settings[Setting::format_custom_row_after_delimiter];
    format_settings.custom.row_before_delimiter = settings[Setting::format_custom_row_before_delimiter];
    format_settings.custom.row_between_delimiter = settings[Setting::format_custom_row_between_delimiter];
    format_settings.custom.try_detect_header = settings[Setting::input_format_custom_detect_header];
    format_settings.custom.skip_trailing_empty_lines = settings[Setting::input_format_custom_skip_trailing_empty_lines];
    format_settings.custom.allow_variable_number_of_columns = settings[Setting::input_format_custom_allow_variable_number_of_columns];
    format_settings.date_time_input_format = settings[Setting::date_time_input_format];
    format_settings.date_time_output_format = settings[Setting::date_time_output_format];
    format_settings.date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = settings[Setting::date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands];
    format_settings.interval_output_format = settings[Setting::interval_output_format];
    format_settings.input_format_ipv4_default_on_conversion_error = settings[Setting::input_format_ipv4_default_on_conversion_error];
    format_settings.input_format_ipv6_default_on_conversion_error = settings[Setting::input_format_ipv6_default_on_conversion_error];
    format_settings.bool_true_representation = settings[Setting::bool_true_representation];
    format_settings.bool_false_representation = settings[Setting::bool_false_representation];
    format_settings.import_nested_json = settings[Setting::input_format_import_nested_json];
    format_settings.input_allow_errors_num = settings[Setting::input_format_allow_errors_num];
    format_settings.input_allow_errors_ratio = settings[Setting::input_format_allow_errors_ratio];
    format_settings.json.max_depth = settings[Setting::input_format_json_max_depth];
    format_settings.json.array_of_rows = settings[Setting::output_format_json_array_of_rows];
    format_settings.json.escape_forward_slashes = settings[Setting::output_format_json_escape_forward_slashes];
    format_settings.json.write_named_tuples_as_objects = settings[Setting::output_format_json_named_tuples_as_objects];
    format_settings.json.skip_null_value_in_named_tuples = settings[Setting::output_format_json_skip_null_value_in_named_tuples];
    format_settings.json.read_named_tuples_as_objects = settings[Setting::input_format_json_named_tuples_as_objects];
    format_settings.json.use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects = settings[Setting::input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects];
    format_settings.json.defaults_for_missing_elements_in_named_tuple = settings[Setting::input_format_json_defaults_for_missing_elements_in_named_tuple];
    format_settings.json.ignore_unknown_keys_in_named_tuple = settings[Setting::input_format_json_ignore_unknown_keys_in_named_tuple];
    format_settings.json.quote_64bit_integers = settings[Setting::output_format_json_quote_64bit_integers];
    format_settings.json.quote_64bit_floats = settings[Setting::output_format_json_quote_64bit_floats];
    format_settings.json.quote_denormals = settings[Setting::output_format_json_quote_denormals];
    format_settings.json.quote_decimals = settings[Setting::output_format_json_quote_decimals];
    format_settings.json.read_bools_as_numbers = settings[Setting::input_format_json_read_bools_as_numbers];
    format_settings.json.read_bools_as_strings = settings[Setting::input_format_json_read_bools_as_strings];
    format_settings.json.read_numbers_as_strings = settings[Setting::input_format_json_read_numbers_as_strings];
    format_settings.json.read_objects_as_strings = settings[Setting::input_format_json_read_objects_as_strings];
    format_settings.json.read_arrays_as_strings = settings[Setting::input_format_json_read_arrays_as_strings];
    format_settings.json.try_infer_numbers_from_strings = settings[Setting::input_format_json_try_infer_numbers_from_strings];
    format_settings.json.infer_incomplete_types_as_strings = settings[Setting::input_format_json_infer_incomplete_types_as_strings];
    format_settings.json.validate_types_from_metadata = settings[Setting::input_format_json_validate_types_from_metadata];
    format_settings.json.validate_utf8 = settings[Setting::output_format_json_validate_utf8];
    format_settings.json_object_each_row.column_for_object_name = settings[Setting::format_json_object_each_row_column_for_object_name];
    format_settings.json.allow_deprecated_object_type = context->getSettingsRef()[Setting::allow_experimental_object_type];
    format_settings.json.allow_json_type = context->getSettingsRef()[Setting::allow_experimental_json_type];
    format_settings.json.compact_allow_variable_number_of_columns = settings[Setting::input_format_json_compact_allow_variable_number_of_columns];
    format_settings.json.try_infer_objects_as_tuples = settings[Setting::input_format_json_try_infer_named_tuples_from_objects];
    format_settings.json.throw_on_bad_escape_sequence = settings[Setting::input_format_json_throw_on_bad_escape_sequence];
    format_settings.json.ignore_unnecessary_fields = settings[Setting::input_format_json_ignore_unnecessary_fields];
    format_settings.json.empty_as_default = settings[Setting::input_format_json_empty_as_default];
    format_settings.json.type_json_skip_duplicated_paths = settings[Setting::type_json_skip_duplicated_paths];
    format_settings.json.pretty_print = settings[Setting::output_format_json_pretty_print];
    format_settings.null_as_default = settings[Setting::input_format_null_as_default];
    format_settings.force_null_for_omitted_fields = settings[Setting::input_format_force_null_for_omitted_fields];
    format_settings.decimal_trailing_zeros = settings[Setting::output_format_decimal_trailing_zeros];
    format_settings.parquet.row_group_rows = settings[Setting::output_format_parquet_row_group_size];
    format_settings.parquet.row_group_bytes = settings[Setting::output_format_parquet_row_group_size_bytes];
    format_settings.parquet.output_version = settings[Setting::output_format_parquet_version];
    format_settings.parquet.case_insensitive_column_matching = settings[Setting::input_format_parquet_case_insensitive_column_matching];
    format_settings.parquet.preserve_order = settings[Setting::input_format_parquet_preserve_order];
    format_settings.parquet.filter_push_down = settings[Setting::input_format_parquet_filter_push_down];
    format_settings.parquet.bloom_filter_push_down = settings[Setting::input_format_parquet_bloom_filter_push_down];
    format_settings.parquet.use_native_reader = settings[Setting::input_format_parquet_use_native_reader];
    format_settings.parquet.enable_json_parsing = settings[Setting::input_format_parquet_enable_json_parsing];
    format_settings.parquet.allow_missing_columns = settings[Setting::input_format_parquet_allow_missing_columns];
    format_settings.parquet.skip_columns_with_unsupported_types_in_schema_inference = settings[Setting::input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference];
    format_settings.parquet.output_string_as_string = settings[Setting::output_format_parquet_string_as_string];
    format_settings.parquet.output_fixed_string_as_fixed_byte_array = settings[Setting::output_format_parquet_fixed_string_as_fixed_byte_array];
    format_settings.parquet.output_datetime_as_uint32 = settings[Setting::output_format_parquet_datetime_as_uint32];
    format_settings.parquet.max_block_size = settings[Setting::input_format_parquet_max_block_size];
    format_settings.parquet.prefer_block_bytes = settings[Setting::input_format_parquet_prefer_block_bytes];
    format_settings.parquet.output_compression_method = settings[Setting::output_format_parquet_compression_method];
    format_settings.parquet.output_compression_level = settings[Setting::output_format_compression_level];
    format_settings.parquet.output_compliant_nested_types = settings[Setting::output_format_parquet_compliant_nested_types];
    format_settings.parquet.use_custom_encoder = settings[Setting::output_format_parquet_use_custom_encoder];
    format_settings.parquet.parallel_encoding = settings[Setting::output_format_parquet_parallel_encoding];
    format_settings.parquet.data_page_size = settings[Setting::output_format_parquet_data_page_size];
    format_settings.parquet.write_batch_size = settings[Setting::output_format_parquet_batch_size];
    format_settings.parquet.write_page_index = settings[Setting::output_format_parquet_write_page_index];
    format_settings.parquet.write_bloom_filter = settings[Setting::output_format_parquet_write_bloom_filter];
    format_settings.parquet.bloom_filter_bits_per_value = settings[Setting::output_format_parquet_bloom_filter_bits_per_value];
    format_settings.parquet.bloom_filter_flush_threshold_bytes = settings[Setting::output_format_parquet_bloom_filter_flush_threshold_bytes];
    format_settings.parquet.local_read_min_bytes_for_seek = settings[Setting::input_format_parquet_local_file_min_bytes_for_seek];
    format_settings.parquet.enable_row_group_prefetch = settings[Setting::input_format_parquet_enable_row_group_prefetch];
    format_settings.parquet.allow_geoparquet_parser = settings[Setting::input_format_parquet_allow_geoparquet_parser];
    format_settings.parquet.write_geometadata = settings[Setting::output_format_parquet_geometadata];
    format_settings.pretty.charset = settings[Setting::output_format_pretty_grid_charset].toString() == "ASCII" ? FormatSettings::Pretty::Charset::ASCII : FormatSettings::Pretty::Charset::UTF8;
    format_settings.pretty.color = settings[Setting::output_format_pretty_color].valueOr(2);
    format_settings.pretty.glue_chunks = settings[Setting::output_format_pretty_glue_chunks].valueOr(2);
    format_settings.pretty.max_column_pad_width = settings[Setting::output_format_pretty_max_column_pad_width];
    format_settings.pretty.max_rows = settings[Setting::output_format_pretty_max_rows];
    format_settings.pretty.max_column_name_width_cut_to = settings[Setting::output_format_pretty_max_column_name_width_cut_to];
    format_settings.pretty.max_column_name_width_min_chars_to_cut = settings[Setting::output_format_pretty_max_column_name_width_min_chars_to_cut];
    format_settings.pretty.max_value_width = settings[Setting::output_format_pretty_max_value_width];
    format_settings.pretty.max_value_width_apply_for_single_value = settings[Setting::output_format_pretty_max_value_width_apply_for_single_value];
    format_settings.pretty.highlight_digit_groups = settings[Setting::output_format_pretty_highlight_digit_groups];
    format_settings.pretty.row_numbers = settings[Setting::output_format_pretty_row_numbers];
    format_settings.pretty.single_large_number_tip_threshold = settings[Setting::output_format_pretty_single_large_number_tip_threshold];
    format_settings.pretty.display_footer_column_names = settings[Setting::output_format_pretty_display_footer_column_names];
    format_settings.pretty.display_footer_column_names_min_rows = settings[Setting::output_format_pretty_display_footer_column_names_min_rows];
    format_settings.pretty.squash_consecutive_ms = settings[Setting::output_format_pretty_squash_consecutive_ms];
    format_settings.pretty.squash_max_wait_ms = settings[Setting::output_format_pretty_squash_max_wait_ms];
    format_settings.pretty.highlight_trailing_spaces = settings[Setting::output_format_pretty_highlight_trailing_spaces];
    format_settings.pretty.multiline_fields = settings[Setting::output_format_pretty_multiline_fields];
    format_settings.pretty.fallback_to_vertical = settings[Setting::output_format_pretty_fallback_to_vertical];
    format_settings.pretty.fallback_to_vertical_max_rows_per_chunk = settings[Setting::output_format_pretty_fallback_to_vertical_max_rows_per_chunk];
    format_settings.pretty.fallback_to_vertical_min_table_width = settings[Setting::output_format_pretty_fallback_to_vertical_min_table_width];
    format_settings.pretty.fallback_to_vertical_min_columns = settings[Setting::output_format_pretty_fallback_to_vertical_min_columns];
    format_settings.protobuf.input_flatten_google_wrappers = settings[Setting::input_format_protobuf_flatten_google_wrappers];
    format_settings.protobuf.output_nullables_with_google_wrappers = settings[Setting::output_format_protobuf_nullables_with_google_wrappers];
    format_settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference = settings[Setting::input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference];
    format_settings.protobuf.use_autogenerated_schema = settings[Setting::format_protobuf_use_autogenerated_schema];
    format_settings.protobuf.google_protos_path = context->getGoogleProtosPath();
    format_settings.regexp.escaping_rule = settings[Setting::format_regexp_escaping_rule];
    format_settings.regexp.regexp = settings[Setting::format_regexp];
    format_settings.regexp.skip_unmatched = settings[Setting::format_regexp_skip_unmatched];
    format_settings.schema.format_schema_source = settings[Setting::format_schema_source];
    format_settings.schema.format_schema = settings[Setting::format_schema];
    format_settings.schema.format_schema_message_name = settings[Setting::format_schema_message_name];
    format_settings.schema.format_schema_path = context->getFormatSchemaPath();
    format_settings.schema.is_server = context->hasGlobalContext() && (context->getGlobalContext()->getApplicationType() == Context::ApplicationType::SERVER);
    format_settings.schema.output_format_schema = settings[Setting::output_format_schema];
    format_settings.skip_unknown_fields = settings[Setting::input_format_skip_unknown_fields];
    format_settings.template_settings.resultset_format = settings[Setting::format_template_resultset];
    format_settings.template_settings.row_between_delimiter = settings[Setting::format_template_rows_between_delimiter];
    format_settings.template_settings.row_format = settings[Setting::format_template_row];
    format_settings.template_settings.row_format_template = settings[Setting::format_template_row_format];
    format_settings.template_settings.resultset_format_template = settings[Setting::format_template_resultset_format];
    format_settings.tsv.crlf_end_of_line = settings[Setting::output_format_tsv_crlf_end_of_line];
    format_settings.tsv.empty_as_default = settings[Setting::input_format_tsv_empty_as_default];
    format_settings.tsv.enum_as_number = settings[Setting::input_format_tsv_enum_as_number];
    format_settings.tsv.null_representation = settings[Setting::format_tsv_null_representation];
    format_settings.tsv.use_best_effort_in_schema_inference = settings[Setting::input_format_tsv_use_best_effort_in_schema_inference];
    format_settings.tsv.skip_first_lines = settings[Setting::input_format_tsv_skip_first_lines];
    format_settings.tsv.try_detect_header = settings[Setting::input_format_tsv_detect_header];
    format_settings.tsv.skip_trailing_empty_lines = settings[Setting::input_format_tsv_skip_trailing_empty_lines];
    format_settings.tsv.allow_variable_number_of_columns = settings[Setting::input_format_tsv_allow_variable_number_of_columns];
    format_settings.tsv.crlf_end_of_line_input = settings[Setting::input_format_tsv_crlf_end_of_line];
    format_settings.values.accurate_types_of_literals = settings[Setting::input_format_values_accurate_types_of_literals];
    format_settings.values.deduce_templates_of_expressions = settings[Setting::input_format_values_deduce_templates_of_expressions];
    format_settings.values.interpret_expressions = settings[Setting::input_format_values_interpret_expressions];
    format_settings.values.escape_quote_with_quote = settings[Setting::output_format_values_escape_quote_with_quote];
    format_settings.with_names_use_header = settings[Setting::input_format_with_names_use_header];
    format_settings.with_types_use_header = settings[Setting::input_format_with_types_use_header];
    format_settings.write_statistics = settings[Setting::output_format_write_statistics];
    format_settings.arrow.low_cardinality_as_dictionary = settings[Setting::output_format_arrow_low_cardinality_as_dictionary];
    format_settings.arrow.use_signed_indexes_for_dictionary = settings[Setting::output_format_arrow_use_signed_indexes_for_dictionary];
    format_settings.arrow.use_64_bit_indexes_for_dictionary = settings[Setting::output_format_arrow_use_64_bit_indexes_for_dictionary];
    format_settings.arrow.allow_missing_columns = settings[Setting::input_format_arrow_allow_missing_columns];
    format_settings.arrow.skip_columns_with_unsupported_types_in_schema_inference = settings[Setting::input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference];
    format_settings.arrow.skip_columns_with_unsupported_types_in_schema_inference = settings[Setting::input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference];
    format_settings.arrow.case_insensitive_column_matching = settings[Setting::input_format_arrow_case_insensitive_column_matching];
    format_settings.arrow.output_string_as_string = settings[Setting::output_format_arrow_string_as_string];
    format_settings.arrow.output_fixed_string_as_fixed_byte_array = settings[Setting::output_format_arrow_fixed_string_as_fixed_byte_array];
    format_settings.arrow.output_compression_method = settings[Setting::output_format_arrow_compression_method];
    format_settings.orc.allow_missing_columns = settings[Setting::input_format_orc_allow_missing_columns];
    format_settings.orc.row_batch_size = settings[Setting::input_format_orc_row_batch_size];
    format_settings.orc.skip_columns_with_unsupported_types_in_schema_inference = settings[Setting::input_format_orc_skip_columns_with_unsupported_types_in_schema_inference];
    format_settings.orc.dictionary_as_low_cardinality = settings[Setting::input_format_orc_dictionary_as_low_cardinality];
    format_settings.orc.case_insensitive_column_matching = settings[Setting::input_format_orc_case_insensitive_column_matching];
    format_settings.orc.output_string_as_string = settings[Setting::output_format_orc_string_as_string];
    format_settings.orc.output_compression_method = settings[Setting::output_format_orc_compression_method];
    format_settings.orc.output_row_index_stride = settings[Setting::output_format_orc_row_index_stride];
    format_settings.orc.output_dictionary_key_size_threshold = settings[Setting::output_format_orc_dictionary_key_size_threshold];
    format_settings.orc.use_fast_decoder = settings[Setting::input_format_orc_use_fast_decoder];
    format_settings.orc.filter_push_down = settings[Setting::input_format_orc_filter_push_down];
    format_settings.orc.reader_time_zone_name = settings[Setting::input_format_orc_reader_time_zone_name];
    format_settings.orc.writer_time_zone_name = settings[Setting::output_format_orc_writer_time_zone_name];
    format_settings.defaults_for_omitted_fields = settings[Setting::input_format_defaults_for_omitted_fields];
    format_settings.capn_proto.enum_comparing_mode = settings[Setting::format_capn_proto_enum_comparising_mode];
    format_settings.capn_proto.skip_fields_with_unsupported_types_in_schema_inference = settings[Setting::input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference];
    format_settings.capn_proto.use_autogenerated_schema = settings[Setting::format_capn_proto_use_autogenerated_schema];
    format_settings.seekable_read = settings[Setting::input_format_allow_seeks];
    format_settings.msgpack.number_of_columns = settings[Setting::input_format_msgpack_number_of_columns];
    format_settings.msgpack.output_uuid_representation = settings[Setting::output_format_msgpack_uuid_representation];
    format_settings.max_rows_to_read_for_schema_inference = settings[Setting::input_format_max_rows_to_read_for_schema_inference];
    format_settings.max_bytes_to_read_for_schema_inference = settings[Setting::input_format_max_bytes_to_read_for_schema_inference];
    format_settings.column_names_for_schema_inference = settings[Setting::column_names_for_schema_inference];
    format_settings.schema_inference_hints = settings[Setting::schema_inference_hints];
    format_settings.schema_inference_make_columns_nullable = settings[Setting::schema_inference_make_columns_nullable].valueOr(2);
    format_settings.schema_inference_make_json_columns_nullable = settings[Setting::schema_inference_make_json_columns_nullable];
    format_settings.mysql_dump.table_name = settings[Setting::input_format_mysql_dump_table_name];
    format_settings.mysql_dump.map_column_names = settings[Setting::input_format_mysql_dump_map_column_names];
    format_settings.sql_insert.max_batch_size = settings[Setting::output_format_sql_insert_max_batch_size];
    format_settings.sql_insert.include_column_names = settings[Setting::output_format_sql_insert_include_column_names];
    format_settings.sql_insert.table_name = settings[Setting::output_format_sql_insert_table_name];
    format_settings.sql_insert.use_replace = settings[Setting::output_format_sql_insert_use_replace];
    format_settings.sql_insert.quote_names = settings[Setting::output_format_sql_insert_quote_names];
    format_settings.try_infer_integers = settings[Setting::input_format_try_infer_integers];
    format_settings.try_infer_dates = settings[Setting::input_format_try_infer_dates];
    format_settings.try_infer_datetimes = settings[Setting::input_format_try_infer_datetimes];
    format_settings.try_infer_datetimes_only_datetime64 = settings[Setting::input_format_try_infer_datetimes_only_datetime64];
    format_settings.try_infer_exponent_floats = settings[Setting::input_format_try_infer_exponent_floats];
    format_settings.markdown.escape_special_characters = settings[Setting::output_format_markdown_escape_special_characters];
    format_settings.bson.output_string_as_string = settings[Setting::output_format_bson_string_as_string];
    format_settings.bson.skip_fields_with_unsupported_types_in_schema_inference = settings[Setting::input_format_bson_skip_fields_with_unsupported_types_in_schema_inference];
    format_settings.binary.max_binary_string_size = settings[Setting::format_binary_max_string_size];
    format_settings.binary.max_binary_array_size = settings[Setting::format_binary_max_array_size];
    format_settings.binary.encode_types_in_binary_format = settings[Setting::output_format_binary_encode_types_in_binary_format];
    format_settings.binary.decode_types_in_binary_format = settings[Setting::input_format_binary_decode_types_in_binary_format];
    format_settings.binary.write_json_as_string = settings[Setting::output_format_binary_write_json_as_string];
    format_settings.binary.read_json_as_string = settings[Setting::input_format_binary_read_json_as_string];
    format_settings.native.allow_types_conversion = settings[Setting::input_format_native_allow_types_conversion];
    format_settings.native.encode_types_in_binary_format = settings[Setting::output_format_native_encode_types_in_binary_format];
    format_settings.native.decode_types_in_binary_format = settings[Setting::input_format_native_decode_types_in_binary_format];
    format_settings.native.write_json_as_string = settings[Setting::output_format_native_write_json_as_string];
    format_settings.native.use_flattened_dynamic_and_json_serialization = settings[Setting::output_format_native_use_flattened_dynamic_and_json_serialization];
    format_settings.max_parser_depth = settings[Setting::max_parser_depth];
    format_settings.date_time_overflow_behavior = settings[Setting::date_time_overflow_behavior];
    format_settings.try_infer_variant = settings[Setting::input_format_try_infer_variants];
    format_settings.client_protocol_version = context->getClientProtocolVersion();
    format_settings.allow_special_bool_values_inside_variant = settings[Setting::allow_special_bool_values_inside_variant];
    format_settings.max_block_size_bytes = settings[Setting::input_format_max_block_size_bytes];

    /// Validate avro_schema_registry_url with RemoteHostFilter when non-empty and in Server context
    if (format_settings.schema.is_server)
    {
        const Poco::URI & avro_schema_registry_url = settings[Setting::format_avro_schema_registry_url];
        if (!avro_schema_registry_url.empty())
            context->getRemoteHostFilter().checkURL(avro_schema_registry_url);
    }

    if (context->getClientInfo().interface == ClientInfo::Interface::HTTP
        && context->getSettingsRef()[Setting::http_write_exception_in_output_format].value)
    {
        format_settings.json.valid_output_on_exception = true;
        format_settings.xml.valid_output_on_exception = true;
    }

    return format_settings;
}


InputFormatPtr FormatFactory::getInput(
    const String & name,
    ReadBuffer & _buf,
    const Block & sample,
    const ContextPtr & context,
    UInt64 max_block_size,
    const std::optional<FormatSettings> & _format_settings,
    std::optional<size_t> _max_parsing_threads,
    std::optional<size_t> _max_download_threads,
    bool is_remote_fs,
    CompressionMethod compression,
    bool need_only_count) const
{
    const auto& creators = getCreators(name);
    if (!creators.input_creator && !creators.random_access_input_creator)
        throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT, "Format {} is not suitable for input", name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    const Settings & settings = context->getSettingsRef();
    size_t max_parsing_threads = _max_parsing_threads.value_or(settings[Setting::max_parsing_threads]);
    size_t max_download_threads = _max_download_threads.value_or(settings[Setting::max_download_threads]);

    RowInputFormatParams row_input_format_params;
    row_input_format_params.max_block_size = max_block_size;
    row_input_format_params.max_block_size_bytes = format_settings.max_block_size_bytes;
    row_input_format_params.allow_errors_num = format_settings.input_allow_errors_num;
    row_input_format_params.allow_errors_ratio = format_settings.input_allow_errors_ratio;
    row_input_format_params.max_execution_time = settings[Setting::max_execution_time];
    row_input_format_params.timeout_overflow_mode = settings[Setting::timeout_overflow_mode];

    if (context->hasQueryContext() && settings[Setting::log_queries])
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Format, name);

    // Add ParallelReadBuffer and decompression if needed.

    auto owned_buf = wrapReadBufferIfNeeded(_buf, compression, creators, format_settings, settings, is_remote_fs, max_download_threads);
    auto & buf = owned_buf ? *owned_buf : _buf;

    // Decide whether to use ParallelParsingInputFormat.

    bool parallel_parsing = max_parsing_threads > 1 && settings[Setting::input_format_parallel_parsing]
        && creators.file_segmentation_engine_creator && !creators.random_access_input_creator && !need_only_count;

    if (settings[Setting::max_memory_usage]
        && settings[Setting::min_chunk_bytes_for_parallel_parsing] * max_parsing_threads * 2 > settings[Setting::max_memory_usage])
        parallel_parsing = false;
    if (settings[Setting::max_memory_usage_for_user]
        && settings[Setting::min_chunk_bytes_for_parallel_parsing] * max_parsing_threads * 2 > settings[Setting::max_memory_usage_for_user])
        parallel_parsing = false;

    if (parallel_parsing)
    {
        const auto & non_trivial_prefix_and_suffix_checker = creators.non_trivial_prefix_and_suffix_checker;
        /// Disable parallel parsing for input formats with non-trivial readPrefix() and readSuffix().
        if (non_trivial_prefix_and_suffix_checker && non_trivial_prefix_and_suffix_checker(buf))
            parallel_parsing = false;
    }

    // Create the InputFormat in one of 3 ways.

    InputFormatPtr format;

    if (parallel_parsing)
    {
        const auto & input_getter = creators.input_creator;

        /// Const reference is copied to lambda.
        auto parser_creator = [input_getter, sample, row_input_format_params, format_settings]
            (ReadBuffer & input) -> InputFormatPtr
            { return input_getter(input, sample, row_input_format_params, format_settings); };

        ParallelParsingInputFormat::Params params{
            buf,
            sample,
            parser_creator,
            creators.file_segmentation_engine_creator,
            name,
            format_settings,
            max_parsing_threads,
            settings[Setting::min_chunk_bytes_for_parallel_parsing],
            max_block_size,
            context->getApplicationType() == Context::ApplicationType::SERVER};

        format = std::make_shared<ParallelParsingInputFormat>(params);
    }
    else if (creators.random_access_input_creator)
    {
        format = creators.random_access_input_creator(
            buf, sample, format_settings, context->getReadSettings(), is_remote_fs, max_download_threads, max_parsing_threads);
    }
    else
    {
        format = creators.input_creator(buf, sample, row_input_format_params, format_settings);
    }

    if (owned_buf)
        format->addBuffer(std::move(owned_buf));
    if (!settings[Setting::input_format_record_errors_file_path].toString().empty())
    {
        if (parallel_parsing)
            format->setErrorsLogger(std::make_shared<ParallelInputFormatErrorsLogger>(context));
        else
            format->setErrorsLogger(std::make_shared<InputFormatErrorsLogger>(context));
    }


    /// It's a kludge. Because I cannot remove context from values format.
    /// (Not needed in the parallel_parsing case above because VALUES format doesn't support it.)
    if (auto * values = typeid_cast<ValuesBlockInputFormat *>(format.get()))
        values->setContext(context);

    return format;
}

std::unique_ptr<ReadBuffer> FormatFactory::wrapReadBufferIfNeeded(
    ReadBuffer & buf,
    CompressionMethod compression,
    const Creators & creators,
    const FormatSettings & format_settings,
    const Settings & settings,
    bool is_remote_fs,
    size_t max_download_threads) const
{
    std::unique_ptr<ReadBuffer> res;

    bool parallel_read = is_remote_fs && max_download_threads > 1 && format_settings.seekable_read && isBufferWithFileSize(buf);
    if (creators.random_access_input_creator)
        parallel_read &= compression != CompressionMethod::None;
    size_t file_size = 0;

    if (parallel_read)
    {
        try
        {
            file_size = getFileSizeFromReadBuffer(buf);
            parallel_read = file_size >= 2 * settings[Setting::max_download_buffer_size];
        }
        catch (const Poco::Exception & e)
        {
            parallel_read = false;
            LOG_TRACE(
                getLogger("FormatFactory"),
                "Failed to setup ParallelReadBuffer because of an exception:\n{}.\n"
                "Falling back to the single-threaded buffer",
                e.displayText());
        }
    }

    if (parallel_read)
    {
        LOG_TRACE(
            getLogger("FormatFactory"),
            "Using ParallelReadBuffer with {} workers with chunks of {} bytes",
            max_download_threads,
            settings[Setting::max_download_buffer_size].value);

        res = wrapInParallelReadBufferIfSupported(
            buf,
            threadPoolCallbackRunnerUnsafe<void>(getIOThreadPool().get(), "ParallelRead"),
            max_download_threads,
            settings[Setting::max_download_buffer_size],
            file_size);
    }

    if (compression != CompressionMethod::None)
    {
        if (!res)
            res = wrapReadBufferReference(buf);
        res = wrapReadBufferWithCompressionMethod(std::move(res), compression, static_cast<int>(settings[Setting::zstd_window_log_max]));
    }

    return res;
}

static void addExistingProgressToOutputFormat(OutputFormatPtr format, const ContextPtr & context)
{
    auto element_id = context->getProcessListElementSafe();
    if (element_id)
    {
        /// While preparing the query there might have been progress (for example in subscalar subqueries) so add it here
        auto current_progress = element_id->getProgressIn();
        Progress read_progress{current_progress.read_rows, current_progress.read_bytes, current_progress.total_rows_to_read};
        if (!read_progress.empty())
            format->setProgress(std::move(read_progress));

        /// Update the start of the statistics to use the start of the query, and not the creation of the format class
        format->setStartTime(element_id->getQueryCPUStartTime(), true);
    }
}

OutputFormatPtr FormatFactory::getOutputFormatParallelIfPossible(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    const ContextPtr & context,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & output_getter = getCreators(name).output_creator;
    if (!output_getter)
        throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT, "Format {} is not suitable for output", name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    const Settings & settings = context->getSettingsRef();

    if (settings[Setting::output_format_parallel_formatting] && getCreators(name).supports_parallel_formatting
        && !settings[Setting::output_format_json_array_of_rows])
    {
        auto formatter_creator = [output_getter, sample, format_settings] (WriteBuffer & output) -> OutputFormatPtr
        {
            return output_getter(output, sample, format_settings);
        };

        ParallelFormattingOutputFormat::Params builder{buf, sample, formatter_creator, settings[Setting::max_threads]};

        if (context->hasQueryContext() && settings[Setting::log_queries])
            context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Format, name);

        auto format = std::make_shared<ParallelFormattingOutputFormat>(builder);
        addExistingProgressToOutputFormat(format, context);
        return format;
    }

    return getOutputFormat(name, buf, sample, context, format_settings);
}


OutputFormatPtr FormatFactory::getOutputFormat(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    const ContextPtr & context,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & output_getter = getCreators(name).output_creator;
    if (!output_getter)
        throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT, "Format {} is not suitable for output", name);

    if (context->hasQueryContext() && context->getSettingsRef()[Setting::log_queries])
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Format, name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    format_settings.max_threads = context->getSettingsRef()[Setting::max_threads];

    auto format = output_getter(buf, sample, format_settings);

    /// It's a kludge. Because I cannot remove context from MySQL format.
    if (auto * mysql = typeid_cast<MySQLOutputFormat *>(format.get()))
        mysql->setContext(context);

    addExistingProgressToOutputFormat(format, context);

    format->setProgressWriteFrequencyMicroseconds(context->getSettingsRef()[Setting::interactive_delay]);

    return format;
}

String FormatFactory::getContentType(const String & name, const std::optional<FormatSettings> & settings) const
{
    return getCreators(name).content_type(settings);
}

SchemaReaderPtr FormatFactory::getSchemaReader(
    const String & name,
    ReadBuffer & buf,
    const ContextPtr & context,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & schema_reader_creator = getCreators(name).schema_reader_creator;
    if (!schema_reader_creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Format {} doesn't support schema inference.", name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    auto schema_reader = schema_reader_creator(buf, format_settings);
    if (schema_reader->needContext())
        schema_reader->setContext(context);
    return schema_reader;
}

ExternalSchemaReaderPtr FormatFactory::getExternalSchemaReader(
    const String & name,
    const ContextPtr & context,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & external_schema_reader_creator = getCreators(name).external_schema_reader_creator;
    if (!external_schema_reader_creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Format {} doesn't support schema inference.", name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    return external_schema_reader_creator(format_settings);
}

void FormatFactory::registerInputFormat(const String & name, InputCreator input_creator)
{
    chassert(input_creator);
    auto & creators = getOrCreateCreators(name);
    if (creators.input_creator || creators.random_access_input_creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Input format {} is already registered", name);
    creators.input_creator = std::move(input_creator);
    registerFileExtension(name, name);
    KnownFormatNames::instance().add(name, /* case_insensitive = */ true);
}

void FormatFactory::registerRandomAccessInputFormat(const String & name, RandomAccessInputCreator input_creator)
{
    chassert(input_creator);
    auto & creators = getOrCreateCreators(name);
    if (creators.input_creator || creators.random_access_input_creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Input format {} is already registered", name);
    creators.random_access_input_creator = std::move(input_creator);
    registerFileExtension(name, name);
    KnownFormatNames::instance().add(name, /* case_insensitive = */ true);
}

void FormatFactory::registerNonTrivialPrefixAndSuffixChecker(const String & name, NonTrivialPrefixAndSuffixChecker non_trivial_prefix_and_suffix_checker)
{
    auto & target = getOrCreateCreators(name).non_trivial_prefix_and_suffix_checker;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Non trivial prefix and suffix checker {} is already registered", name);
    target = std::move(non_trivial_prefix_and_suffix_checker);
}

void FormatFactory::registerAppendSupportChecker(const String & name, AppendSupportChecker append_support_checker)
{
    auto & target = getOrCreateCreators(name).append_support_checker;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Suffix checker {} is already registered", name);
    target = std::move(append_support_checker);
}

void FormatFactory::markFormatHasNoAppendSupport(const String & name)
{
    registerAppendSupportChecker(name, [](const FormatSettings &){ return false; });
}

bool FormatFactory::checkIfFormatSupportAppend(const String & name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings_)
{
    auto format_settings = format_settings_ ? *format_settings_ : getFormatSettings(context);
    const auto & append_support_checker = getCreators(name).append_support_checker;
    /// By default we consider that format supports append
    return !append_support_checker || append_support_checker(format_settings);
}

void FormatFactory::registerOutputFormat(const String & name, OutputCreator output_creator)
{
    auto & target = getOrCreateCreators(name).output_creator;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Output format {} is already registered", name);
    target = std::move(output_creator);
    registerFileExtension(name, name);
    KnownFormatNames::instance().add(name, /* case_insensitive = */ true);
}

void FormatFactory::registerFileExtension(const String & extension, const String & format_name)
{
    file_extension_formats[boost::to_lower_copy(extension)] = format_name;
}

std::optional<String> FormatFactory::tryGetFormatFromFileName(String file_name)
{
    if (file_name == "stdin")
        return tryGetFormatFromFileDescriptor(STDIN_FILENO);

    CompressionMethod compression_method = chooseCompressionMethod(file_name, "");
    if (CompressionMethod::None != compression_method)
    {
        auto pos = file_name.find_last_of('.');
        if (pos != String::npos)
            file_name = file_name.substr(0, pos);
    }

    auto pos = file_name.find_last_of('.');
    if (pos == String::npos)
        return std::nullopt;

    String file_extension = file_name.substr(pos + 1, String::npos);
    boost::algorithm::to_lower(file_extension);
    auto it = file_extension_formats.find(file_extension);
    if (it == file_extension_formats.end())
        return std::nullopt;

    return it->second;
}

String FormatFactory::getFormatFromFileName(String file_name)
{
    if (auto format = tryGetFormatFromFileName(file_name))
        return *format;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine the format of the file {} by it's extension", file_name);
}

std::optional<String> FormatFactory::tryGetFormatFromFileDescriptor(int fd)
{
    std::optional<String> file_name = tryGetFileNameFromFileDescriptor(fd);

    if (file_name)
        return tryGetFormatFromFileName(*file_name);

    return std::nullopt;
}

String FormatFactory::getFormatFromFileDescriptor(int fd)
{
    if (auto format = tryGetFormatFromFileDescriptor(fd))
        return *format;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine the format of the data by the file descriptor {}", fd);
}


void FormatFactory::registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine)
{
    auto & target = getOrCreateCreators(name).file_segmentation_engine_creator;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: File segmentation engine {} is already registered", name);
    auto creator = [file_segmentation_engine](const FormatSettings &)
    {
        return file_segmentation_engine;
    };
    target = std::move(creator);
}

void FormatFactory::registerFileSegmentationEngineCreator(const String & name, FileSegmentationEngineCreator file_segmentation_engine_creator)
{
    auto & target = getOrCreateCreators(name).file_segmentation_engine_creator;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: File segmentation engine creator {} is already registered", name);
    target = std::move(file_segmentation_engine_creator);
}

void FormatFactory::registerSchemaReader(const String & name, SchemaReaderCreator schema_reader_creator)
{
    auto & target = getOrCreateCreators(name).schema_reader_creator;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Schema reader {} is already registered", name);
    target = std::move(schema_reader_creator);
}

void FormatFactory::registerExternalSchemaReader(const String & name, ExternalSchemaReaderCreator external_schema_reader_creator)
{
    auto & target = getOrCreateCreators(name).external_schema_reader_creator;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Schema reader {} is already registered", name);
    target = std::move(external_schema_reader_creator);
}

void FormatFactory::markOutputFormatSupportsParallelFormatting(const String & name)
{
    auto & target = getOrCreateCreators(name).supports_parallel_formatting;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Output format {} is already marked as supporting parallel formatting", name);
    target = true;
}


void FormatFactory::markFormatSupportsSubsetOfColumns(const String & name)
{
    auto & target = getOrCreateCreators(name).subset_of_columns_support_checker;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Format {} is already marked as supporting subset of columns", name);
    target = [](const FormatSettings &){ return true; };
}

void FormatFactory::registerSubsetOfColumnsSupportChecker(const String & name, SubsetOfColumnsSupportChecker subset_of_columns_support_checker)
{
    auto & target = getOrCreateCreators(name).subset_of_columns_support_checker;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Format {} is already marked as supporting subset of columns", name);
    target = std::move(subset_of_columns_support_checker);
}

void FormatFactory::markOutputFormatPrefersLargeBlocks(const String & name)
{
    auto & target = getOrCreateCreators(name).prefers_large_blocks;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Format {} is already marked as preferring large blocks", name);
    target = true;
}

void FormatFactory::markOutputFormatNotTTYFriendly(const String & name)
{
    auto & target = getOrCreateCreators(name).is_tty_friendly;
    if (!target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Format {} is already marked as non-TTY-friendly", name);
    target = false;
}

void FormatFactory::setContentType(const String & name, const String & content_type)
{
    getOrCreateCreators(name).content_type = [=](const std::optional<FormatSettings> &){ return content_type; };
}

void FormatFactory::setContentType(const String & name, ContentTypeGetter content_type)
{
    getOrCreateCreators(name).content_type = content_type;
}

bool FormatFactory::checkIfFormatSupportsSubsetOfColumns(const String & name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings_) const
{
    const auto & target = getCreators(name);
    auto format_settings = format_settings_ ? *format_settings_ : getFormatSettings(context);
    return target.subset_of_columns_support_checker && target.subset_of_columns_support_checker(format_settings);
}

void FormatFactory::registerAdditionalInfoForSchemaCacheGetter(
    const String & name, AdditionalInfoForSchemaCacheGetter additional_info_for_schema_cache_getter)
{
    auto & target = getOrCreateCreators(name).additional_info_for_schema_cache_getter;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: additional info for schema cache getter {} is already registered", name);
    target = std::move(additional_info_for_schema_cache_getter);
}

String FormatFactory::getAdditionalInfoForSchemaCache(const String & name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings_)
{
    const auto & additional_info_getter = getCreators(name).additional_info_for_schema_cache_getter;
    if (!additional_info_getter)
        return "";

    auto format_settings = format_settings_ ? *format_settings_ : getFormatSettings(context);
    return additional_info_getter(format_settings);
}

bool FormatFactory::isInputFormat(const String & name) const
{
    auto it = dict.find(boost::to_lower_copy(name));
    return it != dict.end() && (it->second.input_creator || it->second.random_access_input_creator);
}

bool FormatFactory::isOutputFormat(const String & name) const
{
    auto it = dict.find(boost::to_lower_copy(name));
    return it != dict.end() && it->second.output_creator;
}

bool FormatFactory::checkIfFormatHasSchemaReader(const String & name) const
{
    const auto & target = getCreators(name);
    return bool(target.schema_reader_creator);
}

bool FormatFactory::checkIfFormatHasExternalSchemaReader(const String & name) const
{
    const auto & target = getCreators(name);
    return bool(target.external_schema_reader_creator);
}

bool FormatFactory::checkIfFormatHasAnySchemaReader(const String & name) const
{
    return checkIfFormatHasSchemaReader(name) || checkIfFormatHasExternalSchemaReader(name);
}

bool FormatFactory::checkIfOutputFormatPrefersLargeBlocks(const String & name) const
{
    const auto & target = getCreators(name);
    return target.prefers_large_blocks;
}

bool FormatFactory::checkIfOutputFormatIsTTYFriendly(const String & name) const
{
    const auto & target = getCreators(name);
    return target.is_tty_friendly;
}

bool FormatFactory::checkParallelizeOutputAfterReading(const String & name, const ContextPtr & context) const
{
    auto format_name = boost::to_lower_copy(name);
    if (format_name == "parquet" && context->getSettingsRef()[Setting::input_format_parquet_preserve_order])
        return false;

    return true;
}

void FormatFactory::checkFormatName(const String & name) const
{
    auto it = dict.find(boost::to_lower_copy(name));
    if (it == dict.end())
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown format {}", name);
}

std::vector<String> FormatFactory::getAllInputFormats() const
{
    std::vector<String> input_formats;
    for (const auto & [format_name, creators] : dict)
    {
        if (creators.input_creator || creators.random_access_input_creator)
            input_formats.push_back(format_name);
    }

    return input_formats;
}

FormatFactory & FormatFactory::instance()
{
    static FormatFactory ret;
    return ret;
}

}
