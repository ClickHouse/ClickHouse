#include <cstddef>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/EmptyReadBuffer.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Storages/IStorage.h>
#include <QueryPipeline/Pipe.h>
#include <IO/CompressionMethod.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Core/Settings.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Interpreters/StorageID.h>
#include <Common/quoteString.h>
#include <Parsers/ASTLiteral.h>

#include <algorithm>
#include <unordered_map>

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
    std::string_view data, const String & format_name, const Block & expected_header, const ContextPtr & context)
{
    if (data.empty())
        return {};

    if (format_name.empty() || !FormatFactory::instance().checkIfFormatHasSchemaReader(format_name))
        return {};

    const auto format_settings = getFormatSettings(context);

    /// Ask the format's own schema reader how the real parser identifies columns and validates types,
    /// so the comparison below follows the parser instead of a fixed heuristic. The defaults describe a
    /// positional, value-inferred format whose inferred schema describes what is parsed (the safe,
    /// low-false-positive interpretation) and are used only if the schema reader cannot be created.
    bool format_has_strict_order_of_columns = true;
    bool format_has_exact_types_from_data = false;
    bool format_schema_describes_parsed_data = true;
    bool format_allows_variable_number_of_columns = false;
    try
    {
        auto probe_buffer = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
        auto schema_reader = FormatFactory::instance().getSchemaReader(format_name, *probe_buffer, context, format_settings);

        /// Some formats decide these properties from the data itself — most importantly the
        /// metadata-based `JSON*` formats, which behave differently depending on whether a `meta`
        /// section is present and on `input_format_json_validate_types_from_metadata` — so let the
        /// reader inspect the data first.
        try
        {
            schema_reader->readSchema();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// For every other format the three properties below are constant, so it is Ok to read
            /// them even if this best-effort inspection of the data failed.
        }

        format_has_strict_order_of_columns = schema_reader->hasStrictOrderOfColumns();
        format_has_exact_types_from_data = schema_reader->hasExactTypesFromData();
        format_schema_describes_parsed_data = schema_reader->schemaDescribesParsedData();
        format_allows_variable_number_of_columns = schema_reader->allowVariableNumberOfColumns();
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

    /// Compare structurally with a deliberately loose notion of compatibility. Schema inference widens
    /// types on purpose — numbers become broad types such as `Int64` / `UInt64` / `Float64`, and it does
    /// not reconstruct wrappers such as `Nullable`, `LowCardinality` or `Enum` — so comparing type names
    /// exactly would report a "mismatch" for many inputs that are in fact perfectly insertable (e.g. into
    /// `UInt8`, `Float32` or `LowCardinality(String)` columns). To avoid attaching a misleading
    /// explanation to an unrelated parse error, we treat a column as mismatched only when the inferred
    /// and expected types have no common supertype at all (e.g. a `String` inferred for a numeric column):
    /// a strong, low-false-positive signal that the data really has a different shape than the query expects.
    auto types_are_compatible = [format_has_exact_types_from_data](const DataTypePtr & inferred_type, const DataTypePtr & expected_type)
    {
        if (inferred_type->equals(*expected_type))
            return true;

        /// Self-describing formats (the -WithNamesAndTypes family) carry the declared types in the data,
        /// and the parser validates them against the destination exactly, so any difference is a real
        /// structure mismatch. The loose, supertype-based rule below only makes sense when the types are
        /// inferred (and widened) from the data values.
        if (format_has_exact_types_from_data)
            return false;

        if (tryGetLeastSupertype(DataTypes{inferred_type, expected_type}) != nullptr)
            return true;

        /// Formats that read values from text (e.g. every quoted string in `JSONEachRow`) keep fields as
        /// `String` during schema inference even when the real parser accepts them into richer scalar
        /// types — `UUID`, `IPv4` / `IPv6`, `Enum`, `FixedString`, `Decimal`, dates and times, etc. —
        /// because inference never reconstructs those from a string. The deserializers (`JSONExtractTree`
        /// and the `deserializeText*` family) parse such a string into essentially any scalar destination,
        /// so a `String` inferred there is not a reliable mismatch. Treat it as compatible for every
        /// scalar destination, so a genuine parse error elsewhere in the row does not pick up a misleading
        /// "structure mismatch" suffix. Two kinds of destination are deliberately kept as a mismatch: a
        /// numeric column — `String` inferred where a number is expected is exactly the reliable "text
        /// where a number is expected" signal this diagnostic exists to surface — and a nested/complex
        /// column (`Array`, `Tuple`, `Map`), which genuinely cannot be built from a single scalar string.
        const auto inferred_unwrapped = removeNullable(recursiveRemoveLowCardinality(inferred_type));
        const auto expected_unwrapped = removeNullable(recursiveRemoveLowCardinality(expected_type));
        if (WhichDataType(inferred_unwrapped).isString())
        {
            const WhichDataType which_expected(expected_unwrapped);
            const bool expected_is_numeric = which_expected.isInt() || which_expected.isUInt() || which_expected.isFloat();
            const bool expected_is_nested = which_expected.isArray() || which_expected.isTuple() || which_expected.isMap();
            return !expected_is_numeric && !expected_is_nested;
        }

        return false;
    };

    /// Formats without a strict column order (`JSONEachRow`, `TSKV`) yield named columns whose order
    /// may differ from the destination, so match them against the expected columns by name. Strict-
    /// order formats (`TSV`, `CSV`, `Values`) yield positional placeholder names like `c1`, `c2`, ...
    /// that do not line up with the destination column names, so compare those positionally.
    std::unordered_map<std::string_view, DataTypePtr> expected_by_name;
    for (const auto & column : expected)
        expected_by_name.emplace(column.name, column.type);

    /// Whether the format identifies fields by name is decided by the format itself, mirroring the real
    /// parser. Two cases map fields to columns by name: formats that can read a subset of the destination
    /// columns (`JSONEachRow`, `TSKV`, the `*WithNames*` family when `input_format_with_names_use_header`
    /// is enabled, ...), and formats whose schema reader does not impose a strict column order
    /// (`JSONEachRow`, `TSKV`, `BSONEachRow`, ...). Everything else is compared by position — in
    /// particular a `*WithNames*` format read with the header disabled, where the parser ignores the
    /// file's names and maps columns positionally even though schema inference still reports those names.
    const bool format_reads_by_name = FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name, context, format_settings);

    const bool match_by_name = expected_by_name.size() == expected.size()
        && (format_reads_by_name || !format_has_strict_order_of_columns);

    bool corresponds = true;
    if (match_by_name)
    {
        /// Named formats may legitimately omit columns — they are filled with defaults — and reorder
        /// them, so do not require the counts to be equal: only the columns actually present in the input
        /// have to be type-compatible with their destination. A column missing from the input is not a
        /// structure mismatch.
        for (const auto & column : inferred)
        {
            auto expected_column = expected_by_name.find(column.name);
            if (expected_column == expected_by_name.end())
            {
                /// A field present in the input but unknown to the destination. When
                /// `input_format_skip_unknown_fields` is enabled (the default), the parser legally
                /// skips such fields, so this is not a structure mismatch. When it is disabled, the
                /// parser rejects the row precisely because of the unknown field, and pointing out
                /// the differing structure is accurate.
                if (format_settings.skip_unknown_fields)
                    continue;
                corresponds = false;
                break;
            }
            if (!types_are_compatible(column.type, expected_column->second))
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
        for (auto it_inferred = inferred.begin(), it_expected = expected.begin();
             corresponds && it_inferred != inferred.end() && it_expected != expected.end();
             ++it_inferred, ++it_expected)
        {
            if (!types_are_compatible(it_inferred->type, it_expected->type))
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

String getInsertDataSchemaMismatchDescriptionFromFile(
    const String & file_path,
    const String & compression_method,
    const String & format_name,
    const Block & expected_header,
    const ContextPtr & context)
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

    return getInsertDataSchemaMismatchDescription(prefix, format_name, expected_header, context);
}

void setInsertSchemaMismatchDiagnostic(
    IInputFormat & format, const ASTPtr & ast, const Block & expected_header, const ContextPtr & context)
{
    format.setParseErrorDiagnosticProvider(
        [ast, expected_header, context]() -> String
        {
            /// Only the inline part of the query can be re-read here. The streamed tail (network /
            /// HTTP body) is consumed while parsing and cannot be inspected a second time.
            const auto * insert = ast->as<ASTInsertQuery>();
            if (!insert || !insert->data)
                return {};
            return getInsertDataSchemaMismatchDescription(
                std::string_view(insert->data, insert->end - insert->data), insert->format, expected_header, context);
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

    std::unique_ptr<ReadBuffer> input_buffer = with_buffers
        ? getReadBufferFromASTInsertQuery(ast, context->getSettingsRef()[Setting::snappy_mode])
        : std::make_unique<EmptyReadBuffer>();

    const Settings & settings = context->getSettingsRef();

    /// Create a source from input buffer using format from query
    auto format = context->getInputFormat(ast_insert_query->format, *input_buffer, header,
                                          settings[Setting::max_insert_block_size], std::nullopt,
                                          settings[Setting::max_insert_block_size_bytes],
                                          settings[Setting::min_insert_block_size_rows],
                                          settings[Setting::min_insert_block_size_bytes]);
    format->addBuffer(std::move(input_buffer));

    /// Attach a lazy diagnostic used only if parsing the inserted data fails. Skipped for the
    /// input() table function, whose data comes from a separate source.
    if (with_buffers && !input_function)
        setInsertSchemaMismatchDiagnostic(*format, ast, header, context);

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
