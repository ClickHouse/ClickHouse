#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/EmptyReadBuffer.h>
#include <QueryPipeline/BlockIO.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Formats/IInputFormat.h>
#include "IO/CompressionMethod.h"
#include "Parsers/ASTLiteral.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int UNKNOWN_TYPE_OF_QUERY;
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
        throw Exception("Logical error: query requires data to insert, but it is not INSERT query", ErrorCodes::LOGICAL_ERROR);

    if (ast_insert_query->infile && context->getApplicationType() == Context::ApplicationType::SERVER)
        throw Exception("Query has infile and was send directly to server", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

    if (ast_insert_query->format.empty())
    {
        if (input_function)
            throw Exception("FORMAT must be specified for function input()", ErrorCodes::INVALID_USAGE_OF_INPUT);
        else
            throw Exception("Logical error: INSERT query requires format to be set", ErrorCodes::LOGICAL_ERROR);
    }

    /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.
    auto input_buffer_ast_part = std::make_unique<ReadBufferFromMemory>(
        ast_insert_query->data, ast_insert_query->data ? ast_insert_query->end - ast_insert_query->data : 0);

    std::unique_ptr<ReadBuffer> input_buffer = with_buffers
        ? getReadBufferFromASTInsertQuery(ast)
        : std::make_unique<EmptyReadBuffer>();

    /// Create a source from input buffer using format from query
    auto source = context->getInputFormat(ast_insert_query->format, *input_buffer, header, context->getSettingsRef().max_insert_block_size);
    source->addBuffer(std::move(input_buffer));
    return source;
}

Pipe getSourceFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function)
{
    auto source = getInputFormatFromASTInsertQuery(ast, with_buffers, header, context, input_function);
    Pipe pipe(source);

    const auto * ast_insert_query = ast->as<ASTInsertQuery>();
    if (context->getSettingsRef().input_format_defaults_for_omitted_fields && ast_insert_query->table_id && !input_function)
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(ast_insert_query->table_id, context);
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        const auto & columns = metadata_snapshot->getColumns();
        if (columns.hasDefaults())
        {
            pipe.addSimpleTransform([&](const Block & cur_header)
            {
                return std::make_shared<AddingDefaultsTransform>(cur_header, columns, *source, context);
            });
        }
    }

    return pipe;
}

std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast)
{
    const auto * insert_query = ast->as<ASTInsertQuery>();
    if (!insert_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: query requires data to insert, but it is not INSERT query");

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
        return wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromFile>(in_file), chooseCompressionMethod(in_file, compression_method));
    }

    std::vector<std::unique_ptr<ReadBuffer>> buffers;
    if (insert_query->data)
    {
        /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.
        auto ast_buffer = std::make_unique<ReadBufferFromMemory>(
            insert_query->data, insert_query->end - insert_query->data);

        buffers.emplace_back(std::move(ast_buffer));
    }

    if (insert_query->tail)
        buffers.emplace_back(wrapReadBufferReference(*insert_query->tail));

    return std::make_unique<ConcatReadBuffer>(std::move(buffers));
}

}
