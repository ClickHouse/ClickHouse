#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUInt32, NameToUnixTimestamp, ToNumberMonotonicity<UInt32>>;

}

}
