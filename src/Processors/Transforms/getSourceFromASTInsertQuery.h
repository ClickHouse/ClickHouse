#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <cstddef>
#include <memory>


namespace DB
{

class Pipe;

/// Prepares a input format, which produce data containing in INSERT query.
InputFormatPtr getInputFormatFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function);

/// Prepares a pipe from input format got from ASTInsertQuery,
/// which produce data containing in INSERT query.
Pipe getSourceFromInputFormat(
    const ASTPtr & ast,
    InputFormatPtr format,
    ContextPtr context,
    const ASTPtr & input_function);

/// Prepares a pipe which produce data containing in INSERT query.
Pipe getSourceFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function);

class ReadBuffer;

/// Prepares a read buffer, that allows to read inlined data
/// from ASTInsertQuert directly, and from tail buffer, if it exists.
std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast);

/// Replaces AggregateFunction column with its values or Array of values based on the aggregate_function_input_format
/// The second element of the pair indicates that there are replcaed columns.
std::pair<Block, bool> transformAggregateColumnsToValues(ContextPtr context, const Block & header);

/// Create processor that creates AggregateFunction from value or array based aggregate_function_input_format.
ProcessorPtr createAggregationFromValuesProcessor(ContextPtr context, const SharedHeader & format_header, const Block & header);

}
