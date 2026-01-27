#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeInt64, NameToInt64, ToNumberMonotonicity<Int64>>;

}

}
