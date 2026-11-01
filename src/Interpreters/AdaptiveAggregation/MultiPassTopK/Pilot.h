#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Mode.h>

namespace DB::MultiPassTopKImpl
{

/// The pilot of a multi-pass top-K aggregation: an exact aggregation of a sample of every k-th granule. It starts the
/// candidate mode with the keys of the best ranks of the sample when they stand out and spread over the table, the
/// bounding mode where the sample bounds the top, or else declines.
TopKModePtr makePilot(MultiPassTopKTemplatePtr templ, TopKRunMemoPtr memo);

}
