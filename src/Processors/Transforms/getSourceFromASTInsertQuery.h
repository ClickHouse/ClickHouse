#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context_fwd.h>
#include <cstddef>
#include <memory>

namespace DB
{

class ReadBuffer;
class ASTInsertQuery;
using ReadBuffers = std::vector<std::unique_ptr<ReadBuffer>>;

/** Prepares a pipe which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */
Pipe getSourceFromASTInsertQuery(
        const ASTPtr & ast,
        const Block & header,
        ReadBuffers read_buffers,
        ContextPtr context,
        const ASTPtr & input_function = nullptr);

ReadBuffers getReadBuffersFromASTInsertQuery(const ASTPtr & ast);

}
