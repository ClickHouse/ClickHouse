#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>


namespace DB
{

/** Translates a KQL function call into a ClickHouse AST.
  *
  * `name` must already be lower-cased. On success returns the node; on failure returns
  * nullptr and puts a message in `error` - an unknown function is an ordinary parse
  * outcome here, not an exception, so the caller can attach the source position.
  *
  * A function is either in this registry and translated correctly, or it is rejected by
  * name. There is deliberately no pass-through for unrecognized names: the previous
  * implementation registered 28 functions whose bodies did nothing, which leaked the raw
  * KQL name into the generated SQL and surfaced much later as "Function ... does not exist".
  */
ASTPtr translateKQLFunction(const String & name, const ASTs & arguments, String & error);

/// Whether `name` is a KQL aggregate function, i.e. legal in `summarize` but not elsewhere.
bool isKQLAggregateFunction(const String & name);

/** Builds the predicate for a KQL string operator: `contains`, `has`, `startswith` and the
  * rest, plus their `_cs` case-sensitive variants.
  *
  * These are matching *functions*, not LIKE patterns. Under the previous implementation the
  * needle was pasted between two `%` signs and handed to `ilike`, so `contains '50%'`
  * matched '50x' and a needle containing a quote escaped into the surrounding SQL. Here the
  * needle stays a value and is quoted with `regexpQuoteMeta` at runtime when it reaches a
  * regular expression.
  *
  * Returns nullptr and sets `error` when `op` is not one of them.
  */
ASTPtr buildKQLStringOperator(const String & op, const ASTPtr & haystack, const ASTPtr & needle, String & error);

}
