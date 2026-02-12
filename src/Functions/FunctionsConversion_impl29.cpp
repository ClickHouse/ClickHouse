#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeMacAddress, NameToMacAddress, ToNumberMonotonicity<UInt64>>;

}

}
