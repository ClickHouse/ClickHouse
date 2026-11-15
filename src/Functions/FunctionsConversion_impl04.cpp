#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUInt128, NameToUInt128, ToNumberMonotonicity<UInt128>>;

}

}
