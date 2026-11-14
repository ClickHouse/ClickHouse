#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

/// For testing. With the probability `reattach_tables_before_query_execution_probability`,
/// `DETACH` and `ATTACH` back each table of the current database.
/// It is called before every initial query, regardless of its kind and of the tables it uses.
void reattachRandomTablesInCurrentDatabase(const ContextPtr & context);

}
