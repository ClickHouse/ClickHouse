#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUUID, NameToUUID, ToNumberMonotonicity<UInt128>>;
template class FunctionConvert<DataTypeUUID2, NameToUUID2, ToNumberMonotonicity<UInt128>>;

}

}
