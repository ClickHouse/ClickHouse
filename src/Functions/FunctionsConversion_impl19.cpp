#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeDateTime64, NameToDateTime64, ToDateTimeMonotonicity<DataTypeDateTime64>>;

}

}
