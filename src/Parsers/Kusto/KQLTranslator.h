#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/Kusto/KQLAST.h>


namespace DB
{

/** Lowers a KQL tabular expression onto a ClickHouse `ASTSelectWithUnionQuery`.
  *
  * A KQL pipeline is a sequence of transformations, so each operator either fills a still-empty
  * clause of the select being built or, when the clause it needs is already taken, wraps what
  * exists so far in a subquery and starts a new one. That keeps the mapping from operator to
  * SQL local and obviously order-preserving, which the previous implementation's
  * "walk backwards N tokens and re-lex the pipe" arithmetic did not.
  */
ASTPtr translateKQLQuery(const KQLTabularExpression & query);

}
