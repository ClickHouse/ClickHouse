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
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Common/quoteString.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool input_format_defaults_for_omitted_fields;
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

    ColumnsDescription inferred_columns;
    try
    {
        auto buffer = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
        SingleReadBufferIterator read_buffer_iterator(std::move(buffer));
        inferred_columns = readSchemaFromFormat(format_name, getFormatSettings(context), read_buffer_iterator, context);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// This is a best-effort diagnostic: if we cannot even infer the schema of the data, there
        /// is nothing useful to report, so it is Ok to ignore the exception.
        return {};
    }

    auto inferred = inferred_columns.getAll();
    auto expected = expected_header.getNamesAndTypesList();

    /// Compare structurally, ignoring the artificial Nullable wrapper that schema inference adds by
    /// default, so that inserting valid data into non-nullable columns is not flagged as a mismatch.
    auto core_type_name = [](const DataTypePtr & type) { return removeNullable(type)->getName(); };

    bool corresponds = inferred.size() == expected.size();
    if (corresponds)
    {
        auto it_inferred = inferred.begin();
        auto it_expected = expected.begin();
        for (; it_inferred != inferred.end(); ++it_inferred, ++it_expected)
        {
            if (core_type_name(it_inferred->type) != core_type_name(it_expected->type))
            {
                corresponds = false;
                break;
            }
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
