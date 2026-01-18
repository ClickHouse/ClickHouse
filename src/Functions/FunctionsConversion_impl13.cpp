#include <Functions/FunctionsConversion.h>

namespace DB
{

namespace detail
{

template class FunctionConvert<DataTypeFloat32, NameToFloat32, ToNumberMonotonicity<Float32>>;

}

}
