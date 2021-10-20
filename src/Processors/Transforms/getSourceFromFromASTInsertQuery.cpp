#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Formats/FormatFactory.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromFile.h>
#include <DataStreams/BlockIO.h>
#include <Processors/Transforms/getSourceFromFromASTInsertQuery.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Processors/Pipe.h>
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


Pipe getSourceFromFromASTInsertQuery(
    const ASTPtr & ast,
    ReadBuffer * input_buffer_tail_part,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function)
{
    const auto * ast_insert_query = ast->as<ASTInsertQuery>();

    if (!ast_insert_query)
        throw Exception("Logical error: query requires data to insert, but it is not INSERT query", ErrorCodes::LOGICAL_ERROR);

    if (ast_insert_query->infile && context->getApplicationType() == Context::ApplicationType::SERVER)
        throw Exception("Query has infile and was send directly to server", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

    String format = ast_insert_query->format;
    if (format.empty())
    {
        if (input_function)
            throw Exception("FORMAT must be specified for function input()", ErrorCodes::INVALID_USAGE_OF_INPUT);
        format = "Values";
    }

    /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.

    auto input_buffer_ast_part = std::make_unique<ReadBufferFromMemory>(
        ast_insert_query->data, ast_insert_query->data ? ast_insert_query->end - ast_insert_query->data : 0);

    std::unique_ptr<ReadBuffer> input_buffer;

    if (ast_insert_query->infile)
    {
        /// Data can be from infile
        const auto & in_file_node = ast_insert_query->infile->as<ASTLiteral &>();
        const auto in_file = in_file_node.value.safeGet<std::string>();

        input_buffer = wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromFile>(in_file), chooseCompressionMethod(in_file, ""));
    }
    else
    {
        ConcatReadBuffer::ReadBuffers buffers;
        if (ast_insert_query->data)
            buffers.push_back(input_buffer_ast_part.get());

        if (input_buffer_tail_part)
            buffers.push_back(input_buffer_tail_part);

        /** NOTE Must not read from 'input_buffer_tail_part' before read all between 'ast_insert_query.data' and 'ast_insert_query.end'.
            * - because 'query.data' could refer to memory piece, used as buffer for 'input_buffer_tail_part'.
            */

        input_buffer = std::make_unique<ConcatReadBuffer>(buffers);
    }

    auto source = FormatFactory::instance().getInput(format, *input_buffer, header, context, context->getSettings().max_insert_block_size);
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

    source->addBuffer(std::move(input_buffer_ast_part));
    source->addBuffer(std::move(input_buffer));

    return pipe;
}

}
