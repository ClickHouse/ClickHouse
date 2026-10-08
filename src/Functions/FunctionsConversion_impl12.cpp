#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeBFloat16, NameToBFloat16, ToNumberMonotonicity<BFloat16>>;

}

}
