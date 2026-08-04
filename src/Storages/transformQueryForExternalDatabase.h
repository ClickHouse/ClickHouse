#pragma once

#include <base/types.h>
#include <Core/NamesAndTypes.h>
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
  * Columns listed in `local_only_columns` exist in the external table but their predicates must be
  * evaluated locally (e.g. the external database would compare them differently, so a pushed-down
  * predicate could drop rows the local re-filtering never sees). Their conditions are removed from the
  * remote filter; because removing a disjunct would narrow the remote filter instead of widening it, a
  * disjunction with a branch over such a column is kept local as a whole. Under
  * external_table_strict_query this throws INCORRECT_QUERY, like any other condition that cannot be
  * pushed down.
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
void rejectOuterFilterForQueryBackedExternalSourceIfStrict(const SelectQueryInfo & query_info, const ContextPtr & context);

}
