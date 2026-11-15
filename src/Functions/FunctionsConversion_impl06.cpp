#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeInt8, NameToInt8, ToNumberMonotonicity<Int8>>;

}

}
