#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeFloat64, NameToFloat64, ToNumberMonotonicity<Float64>>;

}

}
