#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


InputStreamFromASTInsertQuery::InputStreamFromASTInsertQuery(
    const ASTPtr & ast, ReadBuffer * input_buffer_tail_part, const Block & header, const Context & context)
{
    const ASTInsertQuery * ast_insert_query = dynamic_cast<const ASTInsertQuery *>(ast.get());

    if (!ast_insert_query)
        throw Exception("Logical error: query requires data to insert, but it is not INSERT query", ErrorCodes::LOGICAL_ERROR);

    String format = ast_insert_query->format;
    if (format.empty())
        format = "Values";

    /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.

    input_buffer_ast_part = std::make_unique<ReadBufferFromMemory>(
        ast_insert_query->data, ast_insert_query->data ? ast_insert_query->end - ast_insert_query->data : 0);

    ConcatReadBuffer::ReadBuffers buffers;
    if (ast_insert_query->data)
        buffers.push_back(input_buffer_ast_part.get());

    if (input_buffer_tail_part)
        buffers.push_back(input_buffer_tail_part);

    /** NOTE Must not read from 'input_buffer_tail_part' before read all between 'ast_insert_query.data' and 'ast_insert_query.end'.
        * - because 'query.data' could refer to memory piece, used as buffer for 'input_buffer_tail_part'.
        */

    input_buffer_contacenated = std::make_unique<ConcatReadBuffer>(buffers);

    res_stream = context.getInputFormat(format, *input_buffer_contacenated, header, context.getSettings().max_insert_block_size);

    auto columns_description = ColumnsDescription::loadFromContext(context, ast_insert_query->database, ast_insert_query->table);
    if (columns_description && !columns_description->defaults.empty())
        res_stream = std::make_shared<AddingDefaultsBlockInputStream>(res_stream, columns_description->defaults, context);
}

}
