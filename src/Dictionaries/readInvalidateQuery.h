#pragma once

#include <string>

namespace DB
{

class Pipe;

/// Using in MySQLDictionarySource and XDBCDictionarySource after processing invalidate_query.
std::string readInvalidateQuery(Pipe pipe);

}
