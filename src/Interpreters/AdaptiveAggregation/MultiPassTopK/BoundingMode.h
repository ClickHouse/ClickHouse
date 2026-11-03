#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Mode.h>

namespace DB::MultiPassTopKImpl
{

/// The bounding mode needs the n-th best rank of the sample to reach this. No group of the top ranks below that rank,
/// so the hot pass takes only the values of the bounding key of at least as many rows.
constexpr UInt64 min_bounding_rank = 8;

/// The bounding mode of a multi-pass top-K aggregation (see `TopKBoundingKey`): the sketch pass, the first restricted
/// pass, and the hot pass where the first restricted pass does not prove the top. `min_key_rows` is the n-th best rank
/// of the sample of the pilot that started the mode, which no group of the top ranks below, or 0 without a pilot.
TopKModePtr makeBoundingMode(MultiPassTopKTemplatePtr templ, TopKRunMemoPtr memo, UInt64 min_key_rows);

}
