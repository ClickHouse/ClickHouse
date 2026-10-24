#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

/// `toStartOfInterval` that ignores the setting `to_start_of_interval_preserves_argument_type`, for `dateTrunc`.
FunctionOverloadResolverPtr createLegacyToStartOfIntervalOverloadResolver(ContextPtr context);

}
