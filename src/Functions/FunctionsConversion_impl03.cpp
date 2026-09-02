#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUInt64, NameToUInt64, ToNumberMonotonicity<UInt64>>;

}

}
