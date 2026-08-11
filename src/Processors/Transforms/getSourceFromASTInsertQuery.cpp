#include <cstddef>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <IO/CompressionMethod.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Core/CaseAwareBlockNameMap.h>
#include <Core/Settings.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/typeid_cast.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Interpreters/StorageID.h>
#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Parsers/ASTLiteral.h>

#include <base/unit.h>

#include <algorithm>
#include <functional>
#include <unordered_set>

namespace DB
{
namespace Setting
{
    extern const SettingsBool input_format_defaults_for_omitted_fields;
    extern const SettingsUInt64 input_format_max_bytes_to_read_for_schema_inference;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 max_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsSnappyMode snappy_mode;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int UNKNOWN_TYPE_OF_QUERY;
}

String getInsertDataSchemaMismatchDescription(
    std::string_view data,
    const String & format_name,
    const Block & expected_header,
    const ContextPtr & context,
    std::optional<size_t> rows_reached_by_parser)
{
    if (data.empty())
        return {};

    if (format_name.empty() || !FormatFactory::instance().checkIfFormatHasSchemaReader(format_name))
        return {};

    auto format_settings = getFormatSettings(context);

    /// Infer only from the rows the parser had reached, including the row whose parsing failed.
    /// Sampling the whole payload would let a row the parser never got to contaminate the diagnosis
    /// of an earlier failure — e.g. `TSV` rows `1\t1.5` and `2\ttext` into `(a UInt8, b UInt8)` fail
    /// on the first row, but the second one would widen the inferred type of `b` to `String` and turn
    /// a value-level error into a bogus structure mismatch.
    if (rows_reached_by_parser)
        format_settings.max_rows_to_read_for_schema_inference
            = std::min<UInt64>(format_settings.max_rows_to_read_for_schema_inference, *rows_reached_by_parser);

    /// Ask the format's own schema reader how the real parser identifies columns and validates types,
    /// so the comparison below follows the parser instead of a fixed heuristic. The defaults describe a
    /// positional, value-inferred format whose inferred schema describes what is parsed (the safe,
    /// low-false-positive interpretation) and are used only if the schema reader cannot be created.
    bool format_has_strict_order_of_columns = true;
    bool format_has_exact_types_from_data = false;
    bool format_schema_describes_parsed_data = true;
    bool format_allows_variable_number_of_columns = false;
    bool format_reads_typed_json_value_tokens = false;
    bool format_reads_string_values_as_whole_text = false;
    bool format_reads_any_value_into_string_column = true;
    bool format_reads_quoted_text_values = false;
    bool format_maps_columns_by_name = false;
    bool format_honors_column_name_matching_mode = false;
    bool format_reads_numeric_into_ipv4 = false;
    bool format_reads_numeric_into_bool = true;
    bool format_stores_typed_numeric_values = false;
    bool format_always_skips_unknown_fields = false;
    try
    {
        auto probe_buffer = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
        auto schema_reader = FormatFactory::instance().getSchemaReader(format_name, *probe_buffer, context, format_settings);

        /// Some formats decide these properties from the data itself — the metadata-based `JSON*`
        /// formats behave differently depending on whether a `meta` section is present and on
        /// `input_format_json_validate_types_from_metadata`, and a plain `CSV` / `TSV` /
        /// `CustomSeparated` may auto-detect a header with column names and even type names in the
        /// data (`*_detect_header`) — so let the reader inspect the data first.
        try
        {
            schema_reader->readSchema();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// For every other format the properties below are constant, so it is Ok to read
            /// them even if this best-effort inspection of the data failed.
        }

        format_has_strict_order_of_columns = schema_reader->hasStrictOrderOfColumns();
        format_has_exact_types_from_data = schema_reader->hasExactTypesFromData();
        format_schema_describes_parsed_data = schema_reader->schemaDescribesParsedData();
        format_allows_variable_number_of_columns = schema_reader->allowVariableNumberOfColumns();
        format_reads_typed_json_value_tokens = schema_reader->readsTypedJSONValueTokens();
        format_reads_string_values_as_whole_text = schema_reader->readsStringValuesAsWholeText();
        format_reads_any_value_into_string_column = schema_reader->readsAnyValueIntoStringColumn();
        format_reads_quoted_text_values = schema_reader->readsQuotedTextValues();
        format_maps_columns_by_name = schema_reader->mapsColumnsByName();
        format_honors_column_name_matching_mode = schema_reader->honorsColumnNameMatchingMode();
        format_reads_numeric_into_ipv4 = schema_reader->readsNumericValueIntoIPv4Column();
        format_reads_numeric_into_bool = schema_reader->readsNumericValueIntoBoolColumn();
        format_stores_typed_numeric_values = schema_reader->storesTypedNumericValues();
        format_always_skips_unknown_fields = schema_reader->alwaysSkipsUnknownFields();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Best-effort: keep the conservative defaults above; it is Ok to ignore the exception.
    }

    /// Some formats read a schema during inference that the real parser then ignores — the
    /// metadata-based `JSON*` formats with `input_format_json_validate_types_from_metadata` = 0 read a
    /// `meta` section that the parser discards, reading the data by value (and positionally for
    /// `JSONCompact`). The inferred structure does not describe what is parsed there, so comparing
    /// against it would attach a misleading explanation to an unrelated value-level parse error.
    if (!format_schema_describes_parsed_data)
        return {};

    ColumnsDescription inferred_columns;
    try
    {
        /// Schema inference reorders the inferred columns to match the destination table for formats
        /// without a strict column order (e.g. `JSONEachRow`, `TSKV`), resolving the table through
        /// `context->getInsertionTable()` in the local `DatabaseCatalog`. On the `clickhouse-client`
        /// path the destination is a remote table that is not registered in the local catalog, so that
        /// lookup would throw and (via the catch below) suppress the whole diagnostic. We do not need
        /// that reordering here — the comparison below matches columns against `expected_header` by name
        /// for such formats — so run inference with the insertion table cleared to avoid the lookup.
        auto inference_context = Context::createCopy(context);
        inference_context->setInsertionTable(StorageID::createEmpty());

        auto buffer = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
        SingleReadBufferIterator read_buffer_iterator(std::move(buffer));
        inferred_columns = readSchemaFromFormat(format_name, format_settings, read_buffer_iterator, inference_context);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// This is a best-effort diagnostic: if we cannot even infer the schema of the data, there
        /// is nothing useful to report, so it is Ok to ignore the exception.
        return {};
    }

    auto inferred = inferred_columns.getAll();
    auto expected = expected_header.getNamesAndTypesList();

    /// Best-effort: if inference produced no columns at all there is nothing useful to compare, so do
    /// not risk attaching a misleading explanation to an unrelated parse error.
    if (inferred.empty())
        return {};

    /// Distinguish a genuinely non-numeric string from a quoted numeric string. Schema inference keeps a
    /// quoted numeric value (the JSON value `"1"`, the CSV field `"1"`, ...) as `String` by default, but
    /// the parser accepts it into a numeric column, so treating every inferred `String` as a mismatch for
    /// a numeric destination would attach a misleading explanation to an unrelated parse error (e.g.
    /// `{"ok": "1", "bad": 1.5}` into `(ok UInt8, bad UInt8)`, where only `bad` is invalid). Infer the
    /// schema a second time with number-from-string inference enabled — the same rule the parser follows
    /// when reading such a value — so a column whose type becomes numeric there is known to hold numeric
    /// content the parser accepts. Aligned by position with `inferred` (both are inferred from the same
    /// data with the same column detection, which the setting does not affect). If this best-effort second
    /// pass is unavailable, `numbers_inference_available` stays false and an inferred `String` is treated
    /// as compatible with a numeric destination, staying on the low-false-positive side.
    bool numbers_inference_available = false;
    std::vector<bool> inferred_is_numeric_content(inferred.size(), false);
    try
    {
        auto number_inference_settings = format_settings;
        number_inference_settings.csv.try_infer_numbers_from_strings = true;
        number_inference_settings.json.try_infer_numbers_from_strings = true;

        auto inference_context = Context::createCopy(context);
        inference_context->setInsertionTable(StorageID::createEmpty());

        auto buffer = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
        SingleReadBufferIterator read_buffer_iterator(std::move(buffer));
        auto inferred_with_numbers
            = readSchemaFromFormat(format_name, number_inference_settings, read_buffer_iterator, inference_context).getAll();

        if (inferred_with_numbers.size() == inferred.size())
        {
            numbers_inference_available = true;
            size_t index = 0;
            for (const auto & column : inferred_with_numbers)
            {
                const auto unwrapped = removeNullable(recursiveRemoveLowCardinality(column.type));
                const WhichDataType which(unwrapped);
                inferred_is_numeric_content[index] = which.isInt() || which.isUInt() || which.isFloat();
                ++index;
            }
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Best-effort: without this second inference we simply do not flag an inferred `String` for a
        /// numeric destination, so it is Ok to ignore the exception here.
    }

    /// True when column `index` of the inferred structure is a `String` that is confirmed to hold
    /// genuinely non-numeric text (a quoted numeric string, which the parser accepts into a numeric
    /// column, has a numeric type in the second inference above and is therefore not confirmed as text).
    auto inferred_is_confirmed_text = [&](size_t index)
    {
        return numbers_inference_available && index < inferred_is_numeric_content.size() && !inferred_is_numeric_content[index];
    };

    /// A `Bool` destination is a special case among the integer-backed types: where the parser re-parses
    /// the field with the `Bool` deserializers, only the literal tokens `1` / `0` (and the word forms)
    /// are accepted, so `2` or `1.5` is a genuine structure mismatch — but schema inference widens both
    /// `1` and `2` to the same `Int64`, so the inferred type alone cannot tell a valid boolean column
    /// from an invalid one. Inspect the actual sampled values instead: lazily parse the sampled data
    /// once with the inferred structure (which is how the values were inferred, so this parse succeeds
    /// where inference did) and record, per top-level column, whether every value is `0` or `1`.
    /// A floating-point `1.0` is counted as a valid boolean literal even though the parser rejects it —
    /// the original literal is not recoverable from the parsed value — staying on the
    /// low-false-positive side. Best-effort like the second inference above: if the sample cannot be
    /// parsed, the answer stays unknown and the caller treats the column as compatible.
    bool sample_values_scanned = false;
    std::vector<std::optional<bool>> sample_column_holds_only_bool_literals;
    auto inferred_column_holds_only_bool_literals = [&](size_t index) -> std::optional<bool>
    {
        if (!sample_values_scanned)
        {
            sample_values_scanned = true;
            sample_column_holds_only_bool_literals.assign(inferred.size(), std::nullopt);

            std::vector<char> is_numeric_column(inferred.size(), false);
            std::vector<char> holds_only_bool_literals(inferred.size(), true);
            {
                size_t column_index = 0;
                for (const auto & column : inferred)
                {
                    const WhichDataType which(removeNullable(recursiveRemoveLowCardinality(column.type)));
                    is_numeric_column[column_index] = which.isInt() || which.isUInt() || which.isFloat();
                    ++column_index;
                }
            }

            size_t rows_read = 0;
            try
            {
                Block header;
                for (const auto & column : inferred)
                    header.insert(ColumnWithTypeAndName(column.type, column.name));

                /// The insertion table is cleared for the same reason as in the schema inference above.
                auto parse_context = Context::createCopy(context);
                parse_context->setInsertionTable(StorageID::createEmpty());

                auto buffer = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
                auto input = parse_context->getInputFormat(format_name, *buffer, header, DEFAULT_BLOCK_SIZE, format_settings);
                QueryPipeline pipeline(std::move(input));
                PullingPipelineExecutor executor(pipeline);

                /// Inspect only the rows schema inference looked at (already clamped above to the rows
                /// the parser had reached), so a value in a row the parser never got to cannot
                /// contaminate the diagnosis of an earlier failure.
                Block block;
                while (rows_read < format_settings.max_rows_to_read_for_schema_inference && executor.pull(block))
                {
                    const size_t rows_to_scan = std::min<size_t>(
                        block.rows(), format_settings.max_rows_to_read_for_schema_inference - rows_read);
                    for (size_t i = 0; i < block.columns() && i < inferred.size(); ++i)
                    {
                        if (!is_numeric_column[i] || !holds_only_bool_literals[i])
                            continue;
                        const auto & column = *block.getByPosition(i).column;
                        for (size_t row = 0; row < rows_to_scan; ++row)
                        {
                            const Field value = column[row];
                            bool is_bool_literal = true;
                            switch (value.getType())
                            {
                                case Field::Types::UInt64:
                                    is_bool_literal = value.safeGet<UInt64>() <= 1;
                                    break;
                                case Field::Types::Int64:
                                {
                                    const auto v = value.safeGet<Int64>();
                                    is_bool_literal = v == 0 || v == 1;
                                    break;
                                }
                                case Field::Types::Float64:
                                {
                                    const auto v = value.safeGet<Float64>();
                                    is_bool_literal = v == 0.0 || v == 1.0;
                                    break;
                                }
                                default:
                                    /// `NULL` and everything else: not evidence of a non-boolean value.
                                    break;
                            }
                            if (!is_bool_literal)
                            {
                                holds_only_bool_literals[i] = false;
                                break;
                            }
                        }
                    }
                    rows_read += block.rows();
                }
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Best-effort: the rows scanned before the failure (e.g. a row truncated by the
                /// sampling byte bound) are still valid evidence, and without any the answer simply
                /// stays unknown, so it is Ok to ignore the exception here.
            }

            if (rows_read)
                for (size_t i = 0; i < inferred.size(); ++i)
                    if (is_numeric_column[i])
                        sample_column_holds_only_bool_literals[i] = static_cast<bool>(holds_only_bool_literals[i]);
        }

        return index < sample_column_holds_only_bool_literals.size() ? sample_column_holds_only_bool_literals[index] : std::nullopt;
    };

    /// Compare structurally with a deliberately loose notion of compatibility. Schema inference widens
    /// types on purpose — numbers become broad types such as `Int64` / `UInt64` / `Float64`, and it does
    /// not reconstruct wrappers such as `Nullable`, `LowCardinality` or `Enum` — so comparing type names
    /// exactly would report a "mismatch" for many inputs that are in fact perfectly insertable (e.g. into
    /// `UInt8`, `Float32` or `LowCardinality(String)` columns). To avoid attaching a misleading
    /// explanation to an unrelated parse error, we treat a column as mismatched only when the inferred
    /// and expected types have no common supertype at all (e.g. a `String` inferred for a numeric column):
    /// a strong, low-false-positive signal that the data really has a different shape than the query expects.
    /// A `std::function` (rather than `auto`) so the structured-token rule below can recurse into the
    /// nested value types with the same loose notion of compatibility. `top_level_column_index` carries
    /// the index of the top-level inferred column so the `Bool` rule below can inspect the sampled
    /// values of that column; the recursive calls pass `std::nullopt` — the sampled values of a nested
    /// element are not individually recoverable, so the rule stays inconclusive (compatible) there.
    std::function<bool(const DataTypePtr &, const DataTypePtr &, bool, std::optional<size_t>)> types_are_compatible
        = [&](const DataTypePtr & inferred_type,
              const DataTypePtr & expected_type,
              bool inferred_is_text,
              std::optional<size_t> top_level_column_index) -> bool
    {
        if (inferred_type->equals(*expected_type))
            return true;

        /// Self-describing formats (the -WithNamesAndTypes family) carry the declared types in the data,
        /// and the parser validates them against the destination exactly, so any difference is a real
        /// structure mismatch. The loose, supertype-based rule below only makes sense when the types are
        /// inferred (and widened) from the data values.
        if (format_has_exact_types_from_data)
            return false;

        const auto inferred_unwrapped = removeNullable(recursiveRemoveLowCardinality(inferred_type));
        const auto expected_unwrapped = removeNullable(recursiveRemoveLowCardinality(expected_type));
        const WhichDataType which_inferred(inferred_unwrapped);
        const WhichDataType which_expected(expected_unwrapped);

        /// A `Dynamic` / `Variant` on either side makes the comparison inconclusive: an expected
        /// `Dynamic` / `Variant` column accepts values of many shapes, and an inferred `Dynamic` /
        /// `Variant` (e.g. schema inference turns a JSON array with elements of different types into
        /// `Array(Dynamic)`) means the actual value types are unknown at the type level. Treat both as
        /// compatible rather than risk a misleading explanation.
        if (which_inferred.isDynamic() || which_inferred.isVariant() || which_expected.isDynamic() || which_expected.isVariant())
            return true;

        const bool inferred_is_numeric = which_inferred.isInt() || which_inferred.isUInt() || which_inferred.isFloat();
        const bool expected_is_nested = which_expected.isArray() || which_expected.isTuple() || which_expected.isMap();

        /// A bare numeric token really is a structure mismatch for a few scalar destinations whose text /
        /// JSON deserializers require a (quoted) string and reject a number in every format — `UUID`, `IPv4`
        /// and `IPv6` (e.g. `{"u": 1}` into `(u UUID)`). This is checked before the supertype rule below
        /// because `IPv4` is backed by a `UInt32` and does share a least supertype with a widened numeric
        /// type, so the supertype rule would otherwise wrongly treat it as compatible. The binary formats
        /// that store typed values are an exception for `IPv4`: `BSONEachRow` reads a BSON `Int32`,
        /// `MsgPack` and `Avro` read an integer straight into the `UInt32`-backed `IPv4` column, and the
        /// formats that cast a decoded source column to the requested type — the columnar `Parquet` /
        /// `Arrow` / `ORC` always, `Native` under `input_format_native_allow_types_conversion` — accept
        /// a numeric column there too (`format_reads_numeric_into_ipv4`), so a numeric value
        /// is valid there and flagging it would be a false positive (`UUID` and `IPv6` still require
        /// binary data of the exact size in those formats, so they stay a mismatch). `FixedString` also rejects a bare number, but only in the typed-token
        /// JSON formats (`SerializationFixedString::deserializeTextJSON` requires a quoted string,
        /// regardless of the `input_format_json_read_numbers_as_strings` setting, which covers only the
        /// plain `String` destination) and in the binary formats that store typed values (`MsgPack`
        /// routes a `FixedString` column only through its string / binary insertion path, and its
        /// integer path has no `FixedString` arm; `BSONEachRow` reads a `FixedString` column only from
        /// the string / binary BSON tags and rejects the numeric ones); `TSV` / `CSV` read the raw
        /// field verbatim into a `FixedString` column, so a number is accepted there and flagging it
        /// would be a false positive.
        ///
        /// A format that reads every field with the quoted-text deserializer (`MySQLDump`, see
        /// `readsQuotedTextValues`) rejects a bare number in every destination whose `deserializeTextQuoted`
        /// requires an opening quote — additionally `String`, `FixedString`, `Date` / `Date32` and `Enum`
        /// (while `DateTime` / `DateTime64` read it as a Unix timestamp and `Decimal` / `Time` / `Time64`
        /// read the number itself, so those stay compatible).
        if (inferred_is_numeric
            && (which_expected.isUUID() || which_expected.isIPv6()
                || (which_expected.isIPv4() && !format_reads_numeric_into_ipv4)
                || ((format_reads_typed_json_value_tokens || format_stores_typed_numeric_values) && which_expected.isFixedString())
                || (format_reads_quoted_text_values
                    && (which_expected.isString() || which_expected.isFixedString() || which_expected.isDateOrDate32()
                        || which_expected.isEnum()))))
            return false;

        /// The mirror image of the "inferred `String`" rule below: a `String` destination accepts values
        /// that schema inference widened to a richer type — but only where the parser actually reads the
        /// raw field into the `String` column. The flat-text parsers do it for every field —
        /// `SerializationString::deserializeText*` for `TSV` / `CSV` takes the raw field verbatim — so
        /// there an inferred non-`String` type going into a `String` destination is never a structure
        /// mismatch (e.g. `1\t1.5` into `(s String, n UInt8)`, where only `n` is invalid). The
        /// typed-token JSON formats accept a bare number / boolean / array / object token into a `String`
        /// column only under the corresponding `input_format_json_read_*_as_strings` setting (all enabled
        /// by default), so for them the inferred token type is a genuine structure mismatch exactly when
        /// the respective setting is disabled. Formats that store typed values (`BSONEachRow`, `MsgPack`)
        /// reject a non-string value for a `String` column outright, so for them any inferred non-`String`
        /// type is a genuine structure mismatch. The quoted-text formats (`readsQuotedTextValues`)
        /// accept a `String` value only when the token was actually quoted: an unquoted bracket or
        /// word token — the only thing the `Quoted` escaping rule's inference can have derived an
        /// `Array` / `Tuple` / `Map` / `Bool` from (`readQuotedFieldInto` tokenizes those forms, and
        /// `tryInferString` maps a quoted token only to `String` or a date / datetime) — is rejected
        /// by `SerializationString::deserializeTextQuoted`, which requires an opening quote, so those
        /// inferred types are a genuine structure mismatch there (e.g. `[1,2]|1.5` into
        /// `(s String, n UInt8)` for `CustomSeparated` with `format_custom_escaping_rule = 'Quoted'`,
        /// where `s` itself fails to parse; a bare number is handled by the quoted-text arm of the
        /// numeric rule above). An inferred `Nothing` (a column of `NULL` literals) stays inconclusive:
        /// with `input_format_null_as_default` (on by default) the quoted-text parser reads `NULL`
        /// into a plain `String` column as the default value. Anything else inferred (a date, a
        /// `UUID`, ... — necessarily inferred from a quoted string token) is read into a `String`
        /// column verbatim in every format, including the quoted-text ones.
        if (which_expected.isString())
        {
            if (format_reads_typed_json_value_tokens)
            {
                if (isBool(inferred_unwrapped))
                    return format_settings.json.read_bools_as_strings;
                if (inferred_is_numeric)
                    return format_settings.json.read_numbers_as_strings;
                if (which_inferred.isArray())
                    return format_settings.json.read_arrays_as_strings;
                if (which_inferred.isTuple() || which_inferred.isMap() || which_inferred.isObject())
                    return format_settings.json.read_objects_as_strings;
                return true;
            }
            if (format_reads_quoted_text_values
                && (which_inferred.isArray() || which_inferred.isTuple() || which_inferred.isMap()
                    || isBool(inferred_unwrapped)))
                return false;
            return format_reads_any_value_into_string_column || which_inferred.isString();
        }

        /// The binary formats that store typed values (`BSONEachRow`, `MsgPack`) keep the on-wire
        /// numeric kind, and their parsers do not convert it across the integer / floating-point
        /// family boundary: a stored double is accepted only into a `Float*` column
        /// (`BSONEachRowRowInputFormat::readAndInsertDouble`, `MsgPackRowInputFormat::insertFloat64`
        /// reject everything else — `UInt8`, `DateTime`, `Enum`, `Decimal`, ...), and a stored
        /// integer is rejected for a `Float*` column in turn. So there — unlike in the text / JSON
        /// formats the supertype rule below is written for, where any numeric token is re-parsed by
        /// the destination's deserializer — an inferred floating-point type is a reliable structure
        /// mismatch for every non-floating-point destination and vice versa. Checked before the
        /// supertype rule, which would otherwise wrongly treat e.g. `Float64` into `UInt8` as
        /// compatible. Integer-to-integer (and integer into the other integer-backed destinations —
        /// dates, `Enum`, ...) stays with the loose rules below: those parsers accept any stored
        /// integer kind there.
        if (format_stores_typed_numeric_values && inferred_is_numeric)
        {
            if (which_inferred.isFloat())
                return which_expected.isFloat();
            if (which_expected.isFloat())
                return false;
        }

        /// A numeric value into a `Bool` destination. Where the parser re-parses the field with the
        /// `Bool` deserializers — the typed-token JSON formats (`SerializationBool::deserializeTextJSON`
        /// accepts only `true` / `false` and `1` / `0`) and the flat-text formats (which additionally
        /// honor `bool_true_representation` / `bool_false_representation`, so the check applies only
        /// when those hold their default word forms and thus cannot legitimize another numeric literal)
        /// — a numeric value other than `0` / `1` is a genuine structure mismatch, but the widened
        /// inferred type (`Int64` for both `1` and `2`) cannot tell them apart, so the sampled values
        /// of the column are inspected instead. Checked before the supertype rule, which would
        /// otherwise wrongly treat e.g. an `Int64` holding `2` as compatible with the `UInt8`-backed
        /// `Bool`. When the values are unavailable (or the format reads numeric values by value into
        /// the `UInt8`-backed column — see `readsNumericValueIntoBoolColumn`), stay compatible.
        if (inferred_is_numeric && isBool(expected_unwrapped))
        {
            const bool parser_accepts_only_bool_literal_tokens = format_reads_typed_json_value_tokens
                || (!format_reads_numeric_into_bool && format_settings.bool_true_representation == "true"
                    && format_settings.bool_false_representation == "false");
            if (parser_accepts_only_bool_literal_tokens && top_level_column_index)
            {
                if (const auto holds_only_bool_literals = inferred_column_holds_only_bool_literals(*top_level_column_index))
                    return *holds_only_bool_literals;
            }
            return true;
        }

        if (tryGetLeastSupertype(DataTypes{inferred_type, expected_type}) != nullptr)
            return true;

        /// The typed-token JSON parsers accept a structured token into more destinations than the
        /// canonical type schema inference reports for it. Inference turns a homogeneous JSON array
        /// into `Array(...)` (`transformTuplesWithEqualNestedTypesToArrays`) and, with
        /// `input_format_json_try_infer_named_tuples_from_objects` enabled (the default), a JSON
        /// object into a named `Tuple` — while the parser still reads the same `[...]` token into an
        /// unnamed `Tuple` positionally (`SerializationTuple::deserializeTextJSONImpl`) and the same
        /// `{...}` token into a `Map` (`SerializationMap::deserializeTextJSON`) or a named `Tuple`
        /// (matching keys to element names). So a structured inferred type is not a reliable mismatch
        /// for such a destination as long as the nested value types are themselves compatible under
        /// the same loose rules, checked recursively. A nested value is a plain token again, so the
        /// top-level text-confirmation flag does not apply below the top level (`false` is passed).
        if (format_reads_typed_json_value_tokens)
        {
            const auto * inferred_tuple = typeid_cast<const DataTypeTuple *>(inferred_unwrapped.get());
            const auto * expected_tuple = typeid_cast<const DataTypeTuple *>(expected_unwrapped.get());
            const auto * inferred_array = typeid_cast<const DataTypeArray *>(inferred_unwrapped.get());
            const auto * expected_array = typeid_cast<const DataTypeArray *>(expected_unwrapped.get());
            const auto * inferred_map = typeid_cast<const DataTypeMap *>(inferred_unwrapped.get());
            const auto * expected_map = typeid_cast<const DataTypeMap *>(expected_unwrapped.get());

            /// The value came from a `[...]` token: inferred as `Array` when the elements were
            /// homogeneous, as an unnamed `Tuple` otherwise.
            const bool inferred_from_array_token = inferred_array || (inferred_tuple && !inferred_tuple->hasExplicitNames());
            /// The value came from a `{...}` token: inferred as a named `Tuple`, as a `Map`, or as
            /// `Object` (its content is then unknown at the type level).
            const bool inferred_from_object_token
                = inferred_map || (inferred_tuple && inferred_tuple->hasExplicitNames()) || which_inferred.isObject();

            if (inferred_from_array_token)
            {
                if (expected_array)
                {
                    const auto & expected_element = expected_array->getNestedType();
                    if (inferred_array)
                        return types_are_compatible(inferred_array->getNestedType(), expected_element, false, std::nullopt);
                    return std::ranges::all_of(
                        inferred_tuple->getElements(),
                        [&](const auto & element) { return types_are_compatible(element, expected_element, false, std::nullopt); });
                }

                /// An unnamed `Tuple` destination reads the `[...]` token positionally. A named `Tuple`
                /// destination does so only when `input_format_json_read_named_tuples_as_objects` is
                /// disabled — with the default (enabled) it requires a `{...}` token, so the array
                /// token stays a mismatch for it.
                if (expected_tuple && (!expected_tuple->hasExplicitNames() || !format_settings.json.read_named_tuples_as_objects))
                {
                    const auto & expected_elements = expected_tuple->getElements();
                    if (inferred_array)
                        return std::ranges::all_of(
                            expected_elements,
                            [&](const auto & element) { return types_are_compatible(inferred_array->getNestedType(), element, false, std::nullopt); });

                    /// The parser requires exactly as many elements in the token as the destination
                    /// `Tuple` has, and an inferred unnamed `Tuple` preserves the element count.
                    const auto & inferred_elements = inferred_tuple->getElements();
                    if (inferred_elements.size() != expected_elements.size())
                        return false;
                    for (size_t i = 0; i < inferred_elements.size(); ++i)
                        if (!types_are_compatible(inferred_elements[i], expected_elements[i], false, std::nullopt))
                            return false;
                    return true;
                }
            }

            if (inferred_from_object_token)
            {
                /// The object keys are string tokens that the `Map` key type parses with its JSON text
                /// deserializer (`SerializationMap::deserializeTextJSONImpl` reads every key with the
                /// key serialization). When the object was inferred as a named `Tuple`, its element
                /// names are the actual keys of the data, so replay each of them through the key
                /// type's deserializer: a key the key type cannot parse (e.g. `"x"` for a
                /// `Map(UInt64, ...)` destination) is a genuine structure mismatch the parser rejects.
                /// For an inferred `Map` / `Object` the actual key strings are unknown at the type
                /// level, so — mirroring the inferred-`String`-into-scalar rule above — the key type is
                /// not checked there. With `input_format_json_read_map_as_array_of_tuples` enabled the
                /// parser reads a `Map` from a `[...]` token instead, so the keys are not replayed
                /// (conservatively compatible). The value types are compared in every case.
                if (expected_map)
                {
                    const auto & expected_value = expected_map->getValueType();
                    if (inferred_tuple)
                    {
                        if (!format_settings.json.read_map_as_array_of_tuples)
                        {
                            const auto & key_type = expected_map->getKeyType();
                            const auto key_serialization = key_type->getDefaultSerialization();
                            for (const auto & key : inferred_tuple->getElementNames())
                            {
                                WriteBufferFromOwnString key_token;
                                writeJSONString(key, key_token, format_settings);
                                ReadBufferFromString key_buffer(key_token.str());
                                auto key_column = key_type->createColumn();
                                if (!key_serialization->tryDeserializeTextJSON(*key_column, key_buffer, format_settings)
                                    || !key_buffer.eof())
                                    return false;
                            }
                        }
                        return std::ranges::all_of(
                            inferred_tuple->getElements(),
                            [&](const auto & element) { return types_are_compatible(element, expected_value, false, std::nullopt); });
                    }
                    if (inferred_map)
                        return types_are_compatible(inferred_map->getValueType(), expected_value, false, std::nullopt);
                    /// Inferred `Object`: the value types are unknown at the type level.
                    return true;
                }

                if (expected_tuple && expected_tuple->hasExplicitNames() && format_settings.json.read_named_tuples_as_objects)
                {
                    /// Keys are matched to the element names; a key absent from the destination is
                    /// skipped when `input_format_json_ignore_unknown_keys_in_named_tuple` is enabled
                    /// (the default) and is a genuine error otherwise, so only then it stays a
                    /// mismatch. Elements of the destination missing from the data are filled with
                    /// defaults by default, so they are not required.
                    if (inferred_tuple)
                    {
                        const auto & expected_names = expected_tuple->getElementNames();
                        for (size_t i = 0; i < inferred_tuple->getElements().size(); ++i)
                        {
                            const auto it = std::ranges::find(expected_names, inferred_tuple->getElementNames()[i]);
                            if (it == expected_names.end())
                            {
                                if (format_settings.json.ignore_unknown_keys_in_named_tuple)
                                    continue;
                                return false;
                            }
                            if (!types_are_compatible(
                                    inferred_tuple->getElements()[i],
                                    expected_tuple->getElements()[it - expected_names.begin()],
                                    false,
                                    std::nullopt))
                                return false;
                        }
                        return true;
                    }
                    /// Inferred `Map` / `Object`: the keys are unknown at the type level, so whether
                    /// they match the element names cannot be decided — treat as compatible.
                    return true;
                }

                /// A `JSON` destination accepts an arbitrary object token.
                if (which_expected.isObject())
                    return true;
            }
        }

        /// Formats that read values from text (e.g. every quoted string in `JSONEachRow`) keep fields as
        /// `String` during schema inference even when the real parser accepts them into richer scalar
        /// types — `UUID`, `IPv4` / `IPv6`, `Enum`, `FixedString`, `Decimal`, dates and times, etc. —
        /// because inference never reconstructs those from a string. The deserializers (`JSONExtractTree`
        /// and the `deserializeText*` family) parse such a string into essentially any scalar destination,
        /// so a `String` inferred there is not a reliable mismatch. Treat it as compatible for every
        /// scalar destination, so a genuine parse error elsewhere in the row does not pick up a misleading
        /// "structure mismatch" suffix. Two kinds of destination are kept as a mismatch: a numeric column
        /// when the inferred `String` is confirmed to hold genuinely non-numeric text (`inferred_is_text`;
        /// a quoted numeric string that the parser accepts into a numeric column is not confirmed as text
        /// and stays compatible) — the reliable "text where a number is expected" signal this diagnostic
        /// exists to surface — and a nested/complex column (`Array`, `Tuple`, `Map`), which genuinely
        /// cannot be built from a single scalar string (except in the whole-text formats, see below).
        if (which_inferred.isString())
        {
            /// `Bool` is backed by `UInt8` but is not a generic numeric destination: its deserializers
            /// accept only the bare literal tokens (`true` / `false` / `1` / `0`, ...), so a string
            /// value is a genuine structure mismatch even when it holds a quoted numeric (`"1"`) that a
            /// real numeric column would accept — the typed-token JSON formats reject any string token
            /// for a `Bool` column, and the `CSV` `Bool` deserializer reads the raw field without
            /// unquoting it. The exception are the formats that re-parse the content of every string
            /// value with the whole-text deserializer of the destination type (`JSONStringsEachRow`,
            /// ...): `SerializationBool::deserializeWholeText` accepts both the quoted numerics
            /// (`"1"` / `"0"`) and the word forms (`"true"` / `"false"`, ...), and the word forms stay
            /// `String` in the numbers-only second inference above, so the generic numeric rule below
            /// would wrongly flag a valid `"true"`. The content of the string is unknown at the type
            /// level, so for the whole-text formats a `String` into `Bool` is treated as compatible.
            if (isBool(expected_unwrapped))
                return format_reads_string_values_as_whole_text;

            /// The same exemption applies to the nested destinations: the whole-text formats re-parse the
            /// content of a string value with the whole-text deserializer of the destination type, and
            /// `Array` / `Tuple` / `Map` all implement it (through `SimpleTextSerialization`), so a string
            /// like `"[1,2]"` is a valid value for an `Array(UInt8)` column there and the content of the
            /// string is unknown at the type level. For the other formats a nested destination genuinely
            /// cannot be built from a single scalar string, so it stays a mismatch.
            if (expected_is_nested)
                return format_reads_string_values_as_whole_text;

            const bool expected_is_numeric = which_expected.isInt() || which_expected.isUInt() || which_expected.isFloat();
            return !(expected_is_numeric && inferred_is_text);
        }

        /// A numeric value that schema inference widened to `Int64` / `UInt64` / `Float64` is accepted by
        /// the text / JSON deserializers into many scalar destinations that share no common supertype with
        /// the widened numeric type: an integer into `DateTime` / `Date` (read as a Unix timestamp), into an
        /// `Enum` (by its numeric value), into `Decimal`, and so on. So an inferred numeric type is not a
        /// reliable structure mismatch for a scalar destination (the string-only `UUID` / `IPv4` / `IPv6`
        /// destinations were already handled above). Only a nested destination (`Array`, `Tuple`, `Map`),
        /// which cannot be built from a single scalar, stays a mismatch.
        if (inferred_is_numeric)
            return !expected_is_nested;

        return false;
    };

    /// Formats without a strict column order (`JSONEachRow`, `TSKV`) yield named columns whose order
    /// may differ from the destination, so match them against the expected columns by name. Strict-
    /// order formats (`TSV`, `CSV`, `Values`) yield positional placeholder names like `c1`, `c2`, ...
    /// that do not line up with the destination column names, so compare those positionally.

    /// Whether the format identifies fields by name is decided by the format's schema reader, mirroring
    /// the real parser. Two cases map fields to columns by name: formats whose schema reader does not
    /// impose a strict column order (`JSONEachRow`, `TSKV`, `BSONEachRow`, ...), and formats whose
    /// schema reader reports that the parser maps columns by name (the `*WithNames*` family when
    /// `input_format_with_names_use_header` is enabled, and the formats that store named columns:
    /// `Native`, `Avro`, `Parquet` / `Arrow` / `ORC`, the named columnar JSON formats, ...). Everything
    /// else is compared by position — in particular a `*WithNames*` format read with the header
    /// disabled, where the parser ignores the file's names and maps columns positionally even though
    /// schema inference still reports those names, and `Npy`, whose parser writes its single column
    /// positionally even though its schema reader always names that column `array` (which is why
    /// `FormatFactory::checkIfFormatSupportsSubsetOfColumns` must not be used as the signal here).
    /// A destination with duplicate column names cannot be matched by name unambiguously (a degenerate
    /// case that does not occur for a real table); compare it positionally, as before.
    std::unordered_set<std::string_view> expected_names;
    for (const auto & column : expected)
        expected_names.insert(column.name);

    const bool match_by_name = expected_names.size() == expected.size()
        && (format_maps_columns_by_name || !format_has_strict_order_of_columns);

    /// With `input_format_import_nested_json` enabled, the row-based JSON parsers map a top-level
    /// object field (`{"n": {...}}`) onto the dotted columns of a `Nested` carrier (`n.i`, ...).
    /// Schema inference cannot represent that mapping — it reports the top-level field as a single
    /// `Tuple` column — so the name-based comparison below would either treat the carrier field as
    /// unknown (hiding a real mismatch inside the nested data) or report a mismatch for perfectly
    /// valid nested input. Skip the diagnostic when that mode can actually apply: the setting is
    /// enabled, the format identifies fields by name and reads JSON values, and the destination has a
    /// dotted column for the parser to register as a `Nested` prefix.
    if (format_settings.import_nested_json && match_by_name
        && (format_reads_typed_json_value_tokens || format_reads_string_values_as_whole_text)
        && std::any_of(
            expected.begin(), expected.end(), [](const auto & column) { return column.name.find('.') != String::npos; }))
        return {};

    bool corresponds = true;
    if (match_by_name)
    {
        /// Resolve names the same way the real parser does. The parsers that resolve field names through
        /// `CaseAwareBlockNameMap` honor `input_format_column_name_matching_mode` (`auto` by default: an
        /// exact-case match first, then a case-insensitive one) — a plain exact lookup would miss, for
        /// example, a `JSONEachRow` field `A` destined for a column `a`, dropping the diagnostic for a
        /// mismatch the parser does detect. But not every by-name parser does that: `TSKV` and `Form`
        /// look names up in a plain hash map and `Native` / `Avro` use `Block::getByName`, all exact
        /// regardless of the setting — there a case-only difference (`A=...` into a column `a`) is a
        /// genuine unknown field the parser rejects (`input_format_skip_unknown_fields` permitting), so
        /// treating it as a match would suppress the explanation. Use the parser's own resolution mode,
        /// reported by the schema reader.
        std::vector<DataTypePtr> expected_types;
        expected_types.reserve(expected.size());
        CaseAwareBlockNameMap expected_by_name(
            format_honors_column_name_matching_mode ? format_settings.input_format_column_matching_case_sensitivity
                                                    : FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE);
        expected_by_name.setSize(expected.size());
        for (const auto & column : expected)
        {
            expected_by_name.add(column.name, expected_types.size());
            expected_types.push_back(column.type);
        }

        /// Named formats may legitimately omit columns — they are filled with defaults — and reorder
        /// them, so do not require the counts to be equal: only the columns actually present in the input
        /// have to be type-compatible with their destination. A column missing from the input is not a
        /// structure mismatch.
        size_t inferred_index = 0;
        for (const auto & column : inferred)
        {
            const size_t current_index = inferred_index++;

            size_t position = CaseAwareBlockNameMap::NOT_FOUND;
            try
            {
                position = expected_by_name.get(column.name);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// A field name that resolves ambiguously under case-insensitive matching (the destination
                /// has columns differing only in case) cannot be attributed to a single column. This is a
                /// rare, pathological destination; stay on the low-false-positive side and do not treat it
                /// as a structure mismatch, so it is Ok to ignore the exception here.
                continue;
            }

            if (position == CaseAwareBlockNameMap::NOT_FOUND)
            {
                /// A field present in the input but unknown to the destination. When
                /// `input_format_skip_unknown_fields` is enabled (the default), the parser legally
                /// skips such fields, so this is not a structure mismatch. The same holds regardless
                /// of the setting for formats whose parser drops unknown fields unconditionally
                /// (`Avro`, the columnar `Parquet` / `Arrow` / `ORC`). Otherwise the parser rejects
                /// the row precisely because of the unknown field (`INCORRECT_DATA`), and pointing
                /// out the differing structure is accurate.
                if (format_settings.skip_unknown_fields || format_always_skips_unknown_fields)
                    continue;
                corresponds = false;
                break;
            }
            if (!types_are_compatible(column.type, expected_types[position], inferred_is_confirmed_text(current_index), current_index))
            {
                corresponds = false;
                break;
            }
        }
    }
    else
    {
        /// Positional formats: each position must be compatible. Formats that legally accept a variable
        /// number of columns (`JSONCompactColumns` always; `CSV` / `TSV` / `CustomSeparated` /
        /// `JSONCompactEachRow` when their `*_allow_variable_number_of_columns` setting is enabled) may
        /// present fewer or more columns than the destination — missing trailing columns are filled with
        /// defaults and/or extra columns are skipped — so for them a differing column count is not by
        /// itself a structure mismatch; only the overlapping positions are compared. For all other
        /// positional formats a differing count is a genuine mismatch.
        if (!format_allows_variable_number_of_columns && inferred.size() != expected.size())
            corresponds = false;
        size_t inferred_index = 0;
        for (auto it_inferred = inferred.begin(), it_expected = expected.begin();
             corresponds && it_inferred != inferred.end() && it_expected != expected.end();
             ++it_inferred, ++it_expected, ++inferred_index)
        {
            if (!types_are_compatible(it_inferred->type, it_expected->type, inferred_is_confirmed_text(inferred_index), inferred_index))
                corresponds = false;
        }
    }

    if (corresponds)
        return {};

    auto format_structure = [](const NamesAndTypesList & columns)
    {
        WriteBufferFromOwnString out;
        for (const auto & column : columns)
            out << "    " << backQuoteIfNeed(column.name) << ' ' << column.type->getName() << '\n';
        return out.str();
    };

    return fmt::format(
        "\nThe structure of the data being inserted does not match the structure expected by the query, "
        "which is likely the cause of the parsing error.\n"
        "Inferred structure of the input data (in format `{}`):\n{}"
        "Expected structure:\n{}",
        format_name, format_structure(inferred), format_structure(expected));
}

namespace
{

/// A row marker found in an exception message: its offset and the number that follows the marker.
struct RowNumberMarker
{
    size_t pos;
    size_t rows;
};

/// Reads a decimal number that follows the LAST occurrence of `prefix` in `message`. When `terminator`
/// is not empty, the number is required to be followed by it.
/// The marker is appended by the format to the end of the message, while the excerpts of the data are
/// part of the original message, so the last occurrence is the one produced by the parser. Matching the
/// first occurrence instead would let the inserted data spoof the row bound.
std::optional<RowNumberMarker> parseRowNumberAfter(std::string_view message, std::string_view prefix, std::string_view terminator)
{
    size_t search_from = message.size();
    while (true)
    {
        size_t marker_pos = message.rfind(prefix, search_from);
        if (marker_pos == std::string_view::npos)
            return {};

        size_t pos = marker_pos + prefix.size();
        size_t rows = 0;
        size_t digits = 0;
        bool overflow = false;
        for (; pos < message.size() && isNumericASCII(message[pos]); ++pos, ++digits)
        {
            if (digits > 18) /// Do not overflow on a bogus message.
            {
                overflow = true;
                break;
            }
            rows = rows * 10 + (message[pos] - '0');
        }

        bool matched = !overflow && digits != 0 && (terminator.empty() || message.substr(pos).starts_with(terminator));
        if (matched)
            return RowNumberMarker{marker_pos, rows};

        /// This occurrence is not a well-formed marker; keep looking for an earlier one.
        if (marker_pos == 0)
            return {};
        search_from = marker_pos - 1;
    }
}

}

std::optional<size_t> getRowsReachedFromParseErrorMessage(std::string_view message)
{
    /// `IInputFormat::generate` appends "(in file/uri <path>)" after the parser's row marker, and the
    /// file name is chosen by the user, so a spoofed marker inside it (e.g. a file named
    /// "data at row 50.tsv") would be the rightmost one. Nothing after that point can be trusted;
    /// cut the search short there. The same substring inside an excerpt of the data can only make the
    /// cut happen before the genuine marker, which degrades to unbounded sampling, not to a wrong bound.
    if (size_t file_name_pos = message.find(": (in file/uri "); file_name_pos != std::string_view::npos)
        message = message.substr(0, file_name_pos);

    /// `IRowInputFormat` appends "(at row N)" where the counter already includes the failing row.
    auto row_input_format_marker = parseRowNumberAfter(message, "(at row ", ")");

    /// `ValuesBlockInputFormat` appends " at row N" where the counter is the number of rows that were
    /// parsed completely, so the parser has reached one row more than that.
    auto values_marker = parseRowNumberAfter(message, " at row ", "");

    /// `ValuesBlockInputFormat` also appends " in one of the first N rows" when the batched evaluation
    /// of templated expressions fails: the failing row is unknown there, but all N rows were read.
    auto values_batch_marker = parseRowNumberAfter(message, " in one of the first ", " rows");

    /// Several forms can be present when the data itself contains something that looks like another
    /// marker - prefer the one that the parser appended, which is the rightmost one.
    std::optional<size_t> rows_reached;
    size_t rightmost_pos = 0;
    auto consider = [&](const std::optional<RowNumberMarker> & marker, size_t bound)
    {
        if (marker && (!rows_reached || marker->pos > rightmost_pos))
        {
            rightmost_pos = marker->pos;
            rows_reached = bound;
        }
    };

    if (row_input_format_marker)
        consider(row_input_format_marker, std::max<size_t>(row_input_format_marker->rows, 1)); /// Be defensive about a zero.
    if (values_marker)
        consider(values_marker, values_marker->rows + 1);
    if (values_batch_marker)
        consider(values_batch_marker, values_batch_marker->rows);

    return rows_reached;
}

String getInsertDataSchemaMismatchDescriptionFromFile(
    const String & file_path,
    const String & compression_method,
    const String & format_name,
    const Block & expected_header,
    const ContextPtr & context,
    std::optional<size_t> rows_reached_by_parser)
{
    if (format_name.empty() || !FormatFactory::instance().checkIfFormatHasSchemaReader(format_name))
        return {};

    String prefix;
    try
    {
        /// Decompress the same way the INSERT itself does (see getReadBufferFromASTInsertQuery), so the
        /// prefix given to schema inference is the actual data.
        auto buffer = wrapReadBufferWithCompressionMethod(
            std::make_unique<ReadBufferFromFile>(file_path),
            chooseCompressionMethod(file_path, compression_method),
            /*zstd_window_log_max=*/ 0,
            context->getSettingsRef()[Setting::snappy_mode]);

        /// Cap the prefix by the same bound schema inference itself uses for sampling.
        size_t max_bytes = context->getSettingsRef()[Setting::input_format_max_bytes_to_read_for_schema_inference];
        while (prefix.size() < max_bytes && !buffer->eof())
        {
            size_t to_copy = std::min(max_bytes - prefix.size(), buffer->available());
            prefix.append(buffer->position(), to_copy);
            buffer->position() += to_copy;
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Best-effort: the path may be a glob matching several files, or the file may have become
        /// unreadable since the failed attempt to insert it. A diagnostic must not raise a new error,
        /// so it is Ok to ignore the exception here and simply skip the extra explanation.
        return {};
    }

    return getInsertDataSchemaMismatchDescription(prefix, format_name, expected_header, context, rows_reached_by_parser);
}

size_t getInsertDataPrefixCaptureLimitForDiagnostic(const ContextPtr & context)
{
    static constexpr size_t max_bytes_to_capture_for_diagnostic = 1_MiB;
    return std::min<size_t>(
        context->getSettingsRef()[Setting::input_format_max_bytes_to_read_for_schema_inference], max_bytes_to_capture_for_diagnostic);
}

void setInsertSchemaMismatchDiagnostic(
    IInputFormat & format, const ASTPtr & ast, const Block & expected_header, const ContextPtr & context)
{
    format.setParseErrorDiagnosticProvider(
        [ast, expected_header, context](std::optional<size_t> rows_reached_by_parser) -> String
        {
            /// Only the inline part of the query can be re-read here. The streamed tail (network /
            /// HTTP body) is consumed while parsing and cannot be inspected a second time.
            const auto * insert = ast->as<ASTInsertQuery>();
            if (!insert || !insert->data)
                return {};
            return getInsertDataSchemaMismatchDescription(
                std::string_view(insert->data, insert->end - insert->data),
                insert->format,
                expected_header,
                context,
                rows_reached_by_parser);
        });
}

PrefixCapturingReadBuffer::PrefixCapturingReadBuffer(ReadBuffer & in_, size_t max_bytes_to_capture_)
    : ReadBuffer(nullptr, 0), in(in_), max_bytes_to_capture(max_bytes_to_capture_)
{
    /// Adopt whatever `in` currently has buffered (possibly already prefetched, e.g. by an earlier
    /// eof() check), rather than assuming it starts out empty.
    working_buffer = in.buffer();
    pos = in.position();
    captureFromCurrentBuffer();
}

void PrefixCapturingReadBuffer::captureFromCurrentBuffer()
{
    if (captured.size() >= max_bytes_to_capture)
        return;

    size_t available = static_cast<size_t>(working_buffer.end() - pos);
    size_t to_copy = std::min(max_bytes_to_capture - captured.size(), available);
    captured.append(pos, to_copy);
}

bool PrefixCapturingReadBuffer::nextImpl()
{
    in.position() = pos;
    bool res = in.next();
    working_buffer = in.buffer();
    pos = in.position();

    if (res)
        captureFromCurrentBuffer();

    return res;
}

bool PrefixCapturingReadBuffer::poll(size_t timeout_microseconds)
{
    /// If the wrapper still has buffered bytes, a read will not block, so data is available now.
    if (hasPendingData())
        return true;

    /// Otherwise a read will pull from the wrapped buffer. Sync its position first — the wrapper
    /// advances `pos` as the format consumes bytes without moving the wrapped buffer's position until
    /// the next `nextImpl` — so the wrapped buffer's own `available()` check reflects the truly
    /// unconsumed bytes rather than a stale range, then delegate the readiness check to it.
    in.position() = pos;
    return in.poll(timeout_microseconds);
}

InputFormatPtr getInputFormatFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function)
{
    /// get ast query
    const auto * ast_insert_query = ast->as<ASTInsertQuery>();

    if (!ast_insert_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query requires data to insert, but it is not INSERT query");

    if (ast_insert_query->infile && context->getApplicationType() == Context::ApplicationType::SERVER)
        throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_QUERY, "Query has infile and was send directly to server");

    if (ast_insert_query->format.empty())
    {
        if (input_function)
            throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "FORMAT must be specified for function input()");
        throw Exception(ErrorCodes::LOGICAL_ERROR, "INSERT query requires format to be set");
    }

    /// Note whether the insert has a streamed tail *before* building the read buffer:
    /// `getReadBufferFromASTInsertQuery` consumes and resets `ASTInsertQuery::tail`.
    const bool has_streamed_tail = ast_insert_query->tail != nullptr;

    std::unique_ptr<ReadBuffer> input_buffer = with_buffers
        ? getReadBufferFromASTInsertQuery(ast, context->getSettingsRef()[Setting::snappy_mode])
        : std::make_unique<EmptyReadBuffer>();

    const Settings & settings = context->getSettingsRef();

    /// The parse-error diagnostic re-reads the inline data of the query (`ASTInsertQuery::data`) to infer
    /// its structure. That is only possible, and only sufficient, when all of the data is inline: as soon
    /// as any of it arrives as a streamed tail (network / HTTP body), those bytes are consumed while
    /// parsing and cannot be inspected a second time, and re-reading only the inline prefix would infer
    /// the structure from an incomplete sample. This covers the tail-only path — the synchronous fallback
    /// of an async insert whose payload exceeded `async_insert_max_data_size` (`executeQuery` moves the
    /// payload out of `ASTInsertQuery::data` into `tail` and nulls `data`) — as well as the mixed path
    /// where the HTTP `query` parameter carries an inline prefix (so `data` is non-null) and the failing
    /// row arrives in the request body (the `tail`). Whenever a tail is present (or there is no inline
    /// data at all), capture a bounded prefix of the bytes as they stream through instead, mirroring the
    /// client stdin path.
    const bool capture_prefix_for_diagnostic
        = with_buffers && !input_function && (has_streamed_tail || !ast_insert_query->data);
    std::unique_ptr<PrefixCapturingReadBuffer> capturing_buffer;
    if (capture_prefix_for_diagnostic)
    {
        /// Unlike the inline (`ASTInsertQuery::data`) and INFILE paths — where the data is re-read only on
        /// the error path — a streamed insert (network / HTTP body / stdin) is consumed while parsing and
        /// cannot be re-read, so the prefix has to be captured eagerly, on every insert, including the ones
        /// that succeed; see getInsertDataPrefixCaptureLimitForDiagnostic for why the capture is bounded.
        capturing_buffer = std::make_unique<PrefixCapturingReadBuffer>(*input_buffer, getInsertDataPrefixCaptureLimitForDiagnostic(context));
    }

    ReadBuffer & format_input = capturing_buffer ? static_cast<ReadBuffer &>(*capturing_buffer) : *input_buffer;

    /// Create a source from input buffer using format from query
    auto format = context->getInputFormat(ast_insert_query->format, format_input, header,
                                          settings[Setting::max_insert_block_size], std::nullopt,
                                          settings[Setting::max_insert_block_size_bytes],
                                          settings[Setting::min_insert_block_size_rows],
                                          settings[Setting::min_insert_block_size_bytes]);

    /// The format reads from the wrapper (when present), which references the wrapped buffer, so both must
    /// be kept alive by the format. Moving the `unique_ptr`s does not move the buffer objects themselves,
    /// so the reference held by the format and `captured_prefix_buffer` below stay valid.
    const PrefixCapturingReadBuffer * captured_prefix_buffer = capturing_buffer.get();
    format->addBuffer(std::move(input_buffer));
    if (capturing_buffer)
        format->addBuffer(std::move(capturing_buffer));

    /// Attach a lazy diagnostic used only if parsing the inserted data fails. Skipped for the
    /// input() table function, whose data comes from a separate source.
    if (with_buffers && !input_function)
    {
        if (captured_prefix_buffer)
            format->setParseErrorDiagnosticProvider(
                [captured_prefix_buffer, expected_header = header, format_name = ast_insert_query->format, context](
                    std::optional<size_t> rows_reached_by_parser) -> String
                {
                    return getInsertDataSchemaMismatchDescription(
                        captured_prefix_buffer->getCapturedPrefix(), format_name, expected_header, context, rows_reached_by_parser);
                });
        else
            setInsertSchemaMismatchDiagnostic(*format, ast, header, context);
    }

    return format;
}

Pipe getSourceFromInputFormat(
    const ASTPtr & ast,
    InputFormatPtr format,
    ContextPtr context,
    const ASTPtr & input_function)
{
    Pipe pipe(format);

    const auto * ast_insert_query = ast->as<ASTInsertQuery>();
    if (context->getSettingsRef()[Setting::input_format_defaults_for_omitted_fields] && !input_function)
    {
        /// Resolve the destination columns. For a plain table the id is in the query;
        /// for a table function (remote(), file(), ...) the id is empty, but the resolved
        /// storage columns (including DEFAULTs) are saved in the context by InterpreterInsertQuery.
        StorageMetadataHandle metadata_snapshot;
        const ColumnsDescription * columns = nullptr;
        if (ast_insert_query->table_id)
        {
            StoragePtr storage = DatabaseCatalog::instance().getTable(ast_insert_query->table_id, context);
            metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
            columns = &metadata_snapshot->getColumns();
        }
        else if (const auto & insertion_columns = context->getInsertionTableColumnsDescription())
        {
            columns = insertion_columns.get();
        }

        if (columns && columns->hasDefaults())
        {
            pipe.addSimpleTransform([&](const SharedHeader & cur_header)
            {
                return std::make_shared<AddingDefaultsTransform>(cur_header, *columns, *format, context);
            });
        }
    }

    return pipe;
}

Pipe getSourceFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function)
{
    auto format = getInputFormatFromASTInsertQuery(ast, with_buffers, header, context, input_function);
    return getSourceFromInputFormat(ast, std::move(format), std::move(context), input_function);
}

std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast, SnappyMode snappy_mode)
{
    const auto * insert_query = ast->as<ASTInsertQuery>();
    if (!insert_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query requires data to insert, but it is not INSERT query");

    if (insert_query->infile)
    {
        /// Data can be from infile
        const auto & in_file_node = insert_query->infile->as<ASTLiteral &>();
        const auto in_file = in_file_node.value.safeGet<std::string>();

        /// It can be compressed and compression method maybe specified in query
        std::string compression_method;
        if (insert_query->compression)
        {
            const auto & compression_method_node = insert_query->compression->as<ASTLiteral &>();
            compression_method = compression_method_node.value.safeGet<std::string>();
        }

        /// Otherwise, it will be detected from file name automatically (by chooseCompressionMethod)
        /// Buffer for reading from file is created and wrapped with appropriate compression method
        return wrapReadBufferWithCompressionMethod(
            std::make_unique<ReadBufferFromFile>(in_file), chooseCompressionMethod(in_file, compression_method),
            /*zstd_window_log_max=*/ 0, snappy_mode);
    }

    ConcatReadBuffer::Buffers buffers;
    if (insert_query->data)
    {
        /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.
        auto ast_buffer = std::make_unique<ReadBufferFromMemory>(
            insert_query->data, insert_query->end - insert_query->data);

        buffers.emplace_back(std::move(ast_buffer));
    }

    /// tail does not possess the input buffer
    if (insert_query->tail)
    {
        buffers.emplace_back(wrapReadBufferPointer(insert_query->tail));
        insert_query->tail.reset();
    }

    chassert(!buffers.empty());
    return std::make_unique<ConcatReadBuffer>(std::move(buffers));
}

}
