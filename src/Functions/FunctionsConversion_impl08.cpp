#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeInt32, NameToInt32, ToNumberMonotonicity<Int32>>;

}

}
