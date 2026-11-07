#pragma once

#include <string>

namespace DB
{

class QueryPipeline;
class RemoteQueryExecutor;

/// Using in MySQLDictionarySource and XDBCDictionarySource after processing invalidate_query.
std::string readInvalidateQuery(QueryPipeline & pipeline);

/// For a remote query whose result structure is not known in advance, so it cannot be a pipeline source:
/// a port with an empty header does not accept the received columns.
std::string readInvalidateQuery(RemoteQueryExecutor & executor);

}
