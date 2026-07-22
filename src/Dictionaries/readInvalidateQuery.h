#pragma once

#include <string>

namespace DB
{

class QueryPipeline;

/// Using in MySQLDictionarySource and XDBCDictionarySource after processing invalidate_query.
std::string readInvalidateQuery(QueryPipeline pipeline);

}
