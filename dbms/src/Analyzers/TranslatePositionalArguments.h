#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class Context;
class WriteBuffer;


/** Transform GROUP BY, ORDER BY and LIMIT BY sections.
  * Replace positional arguments (like ORDER BY 1, 2) to corresponding columns.
  */
struct TranslatePositionalArguments
{
    void process(ASTPtr & ast);
};

}
