#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUInt256, NameToUInt256, ToNumberMonotonicity<UInt256>>;
template class FunctionConvert<DataTypeUInt512, NameToUInt512, ToNumberMonotonicity<UInt512>>;

}

}
