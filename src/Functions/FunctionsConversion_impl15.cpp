#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeDate, NameToDate, ToDateMonotonicity<DataTypeDate>>;

}

}
