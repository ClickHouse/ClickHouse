#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;

}

}
