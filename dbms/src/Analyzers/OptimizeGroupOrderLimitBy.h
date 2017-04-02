#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class Context;
class WriteBuffer;
struct TypeAndConstantInference;


/** Transform GROUP BY, ORDER BY and LIMIT BY sections.
  * (LIMIT BY is an extension to SQL language, do not be confused with ordinary LIMIT)
  *
  * Remove constant expressions (like ORDER BY concat('hello', 'world')).
  * For GROUP BY, unwrap injective functions (like GROUP BY toString(x) -> GROUP BY x).
  * For GROUP BY, remove deterministic functions of another keys (like GROUP BY x + 1, x -> GROUP BY x).
  * TODO For ORDER BY, remove deterministic functions of previous keys (like ORDER BY num, toString(num) -> ORDER BY num),
  *  but only if no collation has specified.
  * As a special case, remove duplicate keys.
  * For LIMIT BY, apply all the same as for GROUP BY.
  *
  * TODO We should apply something similar for DISTINCT,
  *  but keys for DISTINCT are specified implicitly (as whole SELECT expression list).
  *
  * This should be run after CollectAliases, because some aliases will be lost from AST during this transformation.
  * This should be run after TranslatePositionalArguments for positional arguments like ORDER BY 1, 2 not to be confused with constants.
  */
struct OptimizeGroupOrderLimitBy
{
    void process(ASTPtr & ast, TypeAndConstantInference & expression_info);
};

}
