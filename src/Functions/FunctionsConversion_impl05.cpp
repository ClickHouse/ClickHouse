#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUInt256, NameToUInt256, ToNumberMonotonicity<UInt256>>;

}

}
