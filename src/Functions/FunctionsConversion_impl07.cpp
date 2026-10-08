#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeInt16, NameToInt16, ToNumberMonotonicity<Int16>>;

}

}
