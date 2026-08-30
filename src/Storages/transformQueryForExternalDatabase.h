#pragma once

#include <base/types.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class IAST;

/** For given ClickHouse query,
  * creates another query in a form of
  *
  * SELECT columns... FROM db.table WHERE ...
  *
  * where 'columns' are all required columns to read from "left" table of original query,
  * and WHERE contains subset of (AND-ed) conditions from original query,
  * that contain only compatible expressions.
  *
  * If limit is passed additionally apply LIMIT in result query.
  *
  * Functions listed in `unsupported_functions` are kept for local filtering even if they are otherwise
  * compatible. This lets a caller exclude operators whose semantics differ in its external database.
  *
  * Columns listed in `local_only_columns` belong to this source but their predicates must be evaluated
  * locally. This includes external columns whose comparison semantics differ and plan-time virtual columns
  * that do not exist in the external database. Their conditions are removed from the remote filter; because
  * removing a disjunct would narrow the remote filter instead of widening it, a disjunction with a branch
  * over such a column is kept local as a whole. Under `external_table_strict_query` this throws
  * `INCORRECT_QUERY`, like any other condition that cannot be pushed down.
  *
  * Compatible expressions are comparisons of identifiers, constants, and logical operations on them.
  *
  * Throws INCORRECT_QUERY if external_table_strict_query (from context settings)
  * is set and some expression from WHERE is not compatible.
  */
String transformQueryForExternalDatabase(
    const SelectQueryInfo & query_info,
    const Names & column_names,
    const NamesAndTypesList & available_columns,
    IdentifierQuotingStyle identifier_quoting_style,
    LiteralEscapingStyle literal_escaping_style,
    const String & database,
    const String & table,
    const StorageID & source_storage_id,
    ContextPtr context,
    std::optional<size_t> limit = {},
    const NameSet & unsupported_functions = {},
    const NameSet & local_only_columns = {});

/** When the data source of an external database integration is a user-provided query (passed to the external
  * database as is), the query is not rewritten by `transformQueryForExternalDatabase` and no outer predicate can
  * be pushed down into it. Under `external_table_strict_query = 1` the contract is that an outer filter that
  * cannot be executed remotely must fail instead of being silently applied locally in ClickHouse. This throws
  * INCORRECT_QUERY when strict mode is enabled and the outer query has a filter on the source; otherwise it does
  * nothing (the filter is applied locally, as usual).
  */
void rejectOuterFilterForQueryBackedExternalSourceIfStrict(
    const SelectQueryInfo & query_info,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    const StorageID & source_storage_id,
    const NameSet & local_only_columns = {});

/** Recursively normalize `node` so that it re-serializes into SQL the external database can parse. Used for
  * user-provided `(SELECT ...)` subqueries that are formatted from the raw AST and therefore bypass the
  * normalization done by `transformQueryForExternalDatabase`:
  * - single-row multi-column `IN`/`NOT IN` sets (e.g. `(a, b) IN ((1, 'x'))`) keep their outer parentheses
  *   instead of collapsing to a flat scalar list (`IN (1, 'x')`);
  * - the internal `_CAST(literal, 'Type')` wrapper that the analyzer's `ConstantNode::toAST` puts around
  *   literals whose type does not survive the text round trip is unwrapped back to the plain literal;
  * - the `tuple` function with at least two arguments is marked to be formatted in the parenthesized
  *   operator form `(a, b)` (a row value in MySQL / PostgreSQL / SQLite) instead of the ClickHouse-only
  *   call form `tuple(a, b)` - but only in positions where those databases accept a row value, i.e. as
  *   an operand of a comparison or `IN`; in any other position (e.g. the SELECT list) both the `tuple`
  *   call and the equivalent tuple literal throw `BAD_ARGUMENTS`, because the row value would be a
  *   syntax error for the external database there (SQLite reports "row value misused");
  * - expressions that only have a ClickHouse-specific text form (`tuple` with fewer than two arguments,
  *   `array`, `map`, and - for the `Regular` escaping style, where no dialect field visitor rejects them
  *   at format time - literals containing an `Array` / `Map` or a tuple with fewer than two elements)
  *   throw `BAD_ARGUMENTS` instead of being sent to the external database as SQL it cannot parse.
  */
void normalizeSubqueryForExternalDatabase(ASTPtr & node, LiteralEscapingStyle literal_escaping_style);

}
