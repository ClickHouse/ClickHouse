#pragma once

#include <Core/Field.h>
#include <Functions/IFunction.h>

namespace DB
{

FunctionOverloadResolverPtr createExponentialTimeDecayingFunction(
    const Array & parameters,
    ContextPtr context);

}
