#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeIPv6, NameToIPv6, ToNumberMonotonicity<UInt128>>;

}

}
