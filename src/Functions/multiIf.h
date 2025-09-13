#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalMultiIfOverloadResolver(
    bool allow_execute_multiif_columnar,
    bool use_variant_as_common_type,
    bool optimize_if_transform_const_strings_to_lowcardinality);

}
