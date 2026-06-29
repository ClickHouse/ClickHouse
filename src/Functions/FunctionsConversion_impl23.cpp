#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;

}

}
