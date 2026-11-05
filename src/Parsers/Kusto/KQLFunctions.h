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
  * A name resolves in one of three ways, in this order:
  *
  *  1. It is in the registry, and is translated with the semantics Kusto documents.
  *  2. It is a Kusto name this dialect does not implement (`isUnsupportedKQLFunction`), and
  *     is rejected. A Kusto name must never quietly mean something else.
  *  3. Otherwise it is taken to be a ClickHouse function, so a KQL query can reach the rest
  *     of ClickHouse. An unknown name is then reported by the analyzer, which also suggests
  *     near misses.
  *
  * `name` is the call lower-cased; `original_name` is the spelling the user wrote, which is
  * what case 3 passes on, because ClickHouse function names are case-sensitive.
  *
  * Kusto allows aggregate functions in an aggregation context only, so a call to one is
  * rejected unless `allow_aggregates` says the caller is parsing one - the aggregation list
  * of `summarize`.
  */
ASTPtr translateKQLFunction(const String & name, const String & original_name, const ASTs & arguments, bool allow_aggregates, String & error);

/// Kusto function names this dialect does not implement.
///
/// These are rejected rather than passed through, because a Kusto name must never quietly
/// mean something else: ClickHouse also has a `range`, and `range(1, 3, 1)` is `[1, 2]` here
/// and `[1, 2, 3]` in Kusto.
bool isUnsupportedKQLFunction(const String & name);

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

/** Builds `left` equals `right`, ignoring case - what `=~`, `!~` and `in~` compare with.
  *
  * Kusto compares ordinally, which is what the `*CaseInsensitiveUTF8` search functions do.
  * They are used in preference to lower-casing both sides, because `lowerUTF8` requires ICU
  * and would make the operator unavailable in a build without it.
  */
ASTPtr kqlCaseInsensitiveEquals(const ASTPtr & left, const ASTPtr & right);

}
