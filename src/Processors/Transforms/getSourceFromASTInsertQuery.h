#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <IO/SnappyMode.h>
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
/// `snappy_mode` selects the snappy framing used when the data (e.g. `INSERT ... FROM INFILE`) is snappy-compressed.
std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast, SnappyMode snappy_mode = SnappyMode::Basic);

}
