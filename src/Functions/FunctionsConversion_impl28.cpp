#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeDecimal<Decimal256>, NameToDecimal256, UnknownMonotonicity>;

}

}
