#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Whether `ast` contains a call to `arrayJoin` - the one function that changes the number of rows of
/// the block it is evaluated over. An alias of it counts (`unnest` is registered as a case-insensitive
/// one), and a nested query does not: its `arrayJoin` multiplies the rows of that query and hands the
/// enclosing expression whatever shape the query returns.
///
/// `descend_into_sql_udfs` walks the body of a SQL UDF the expression calls as well. It is for an
/// expression that is stored as written and has its UDFs inlined only when it is evaluated - a row
/// policy filter; a table definition has them substituted before it is checked.
bool astContainsArrayJoin(const IAST & ast, bool descend_into_sql_udfs = false);

}
