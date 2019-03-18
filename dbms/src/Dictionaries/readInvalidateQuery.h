#pragma once
#include <string>

namespace DB
{

class IBlockInputStream;

/// Using in MySQLDictionarySource and XDBCDictionarySource after processing invalidate_query.
std::string readInvalidateQuery(IBlockInputStream & block_input_stream);

}
