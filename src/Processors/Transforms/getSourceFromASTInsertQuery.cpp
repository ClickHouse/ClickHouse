#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Formats/FormatFactory.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <DataStreams/BlockIO.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Processors/Pipe.h>
#include <Processors/Formats/IInputFormat.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_USAGE_OF_INPUT;
}

Pipe getSourceFromASTInsertQuery(
    const ASTPtr & ast,
    const Block & header,
    ReadBuffers read_buffers,
    ContextPtr context,
    const ASTPtr & input_function)
{
    const auto * ast_insert_query = ast->as<ASTInsertQuery>();
    if (!ast_insert_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: query requires data to insert, but it is not INSERT query");

    if (read_buffers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Required at least one read buffer to create source from ASTInsertQuery");

    String format = ast_insert_query->format;
    if (format.empty())
    {
        if (input_function)
            throw Exception("FORMAT must be specified for function input()", ErrorCodes::INVALID_USAGE_OF_INPUT);
        format = "Values";
    }

    auto input_buffer = std::make_unique<ConcatReadBuffer>(std::move(read_buffers));
    auto source = FormatFactory::instance().getInput(
        format, *input_buffer, header,
        context, context->getSettings().max_insert_block_size);

    source->addBuffer(std::move(input_buffer));

    Pipe pipe(source);
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

ReadBuffers getReadBuffersFromASTInsertQuery(const ASTPtr & ast)
{
    const auto * insert_query = ast->as<ASTInsertQuery>();
    if (!insert_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: query requires data to insert, but it is not INSERT query");

    ReadBuffers buffers;
    if (insert_query->data)
    {
        /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.
        auto ast_buffer = std::make_unique<ReadBufferFromMemory>(
            insert_query->data, insert_query->end - insert_query->data);

        buffers.emplace_back(std::move(ast_buffer));
    }

    if (insert_query->tail)
        buffers.emplace_back(wrapReadBufferReference(*insert_query->tail));

    return buffers;
}

}
