#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeIPv4, NameToIPv4, ToNumberMonotonicity<UInt32>>;

}

}
