#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeDate32, NameToDate32, ToDateMonotonicity<DataTypeDate32>>;

}

}
