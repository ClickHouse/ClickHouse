#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeUUID, NameToUUID, ToNumberMonotonicity<UInt128>>;

}

}
