#pragma once
#include <string>

class IProfilingBlockInputStream;

namespace DB
{

// Using in MySQLDictionarySource and ODBCDictionarySource after processing invalidate_query
std::string readInvalidateQuery(IProfilingBlockInputStream & block_input_stream);


}
