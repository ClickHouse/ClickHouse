#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeInt128, NameToInt128, ToNumberMonotonicity<Int128>>;

}

}
