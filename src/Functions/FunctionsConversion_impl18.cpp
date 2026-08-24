#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeDateTime, NameToDateTime32, ToDateTimeMonotonicity<DataTypeDateTime>>;

}

}
