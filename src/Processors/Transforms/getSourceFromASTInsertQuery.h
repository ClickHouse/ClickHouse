#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <cstddef>
#include <memory>


namespace DB
{

/** Prepares a pipe which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */

class Pipe;

std::pair<InputFormatPtr, Pipe> getSourceFromASTInsertQuery(
        const ASTPtr & ast,
        bool with_buffers,
        const Block & header,
        ContextPtr context,
        const ASTPtr & input_function);

class ReadBuffer;
std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast);

}
