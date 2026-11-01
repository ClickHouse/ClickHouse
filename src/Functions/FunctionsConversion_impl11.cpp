#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeInt256, NameToInt256, ToNumberMonotonicity<Int256>>;

}

}
