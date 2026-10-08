#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;

}

}
