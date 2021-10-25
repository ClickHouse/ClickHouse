#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context_fwd.h>
#include <cstddef>
#include <memory>


namespace DB
{

/** Prepares a pipe which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */

class Pipe;

Pipe getSourceFromFromASTInsertQuery(
        const ASTPtr & ast,
        ReadBuffer * input_buffer_tail_part,
        const Block & header,
        ContextPtr context,
        const ASTPtr & input_function);

}
