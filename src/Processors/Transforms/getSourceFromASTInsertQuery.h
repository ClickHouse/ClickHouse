#pragma once

#include <Parsers/IAST.h>
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

}
