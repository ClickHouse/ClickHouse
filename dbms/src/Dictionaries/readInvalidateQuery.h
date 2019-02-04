#pragma once
#include <string>

class IBlockInputStream;

namespace DB
{
// Using in MySQLDictionarySource and XDBCDictionarySource after processing invalidate_query
std::string readInvalidateQuery(IBlockInputStream & block_input_stream);


}
