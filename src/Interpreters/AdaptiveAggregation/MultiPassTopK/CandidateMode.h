#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Mode.h>

namespace DB::MultiPassTopKImpl
{

/// The candidate mode of a multi-pass top-K aggregation with the candidates `candidate_keys`: the candidate pass, and
/// the fallback over the hot cells where the candidates do not prove the top (see `TopKCandidates`).
TopKModePtr makeCandidateMode(MultiPassTopKTemplatePtr templ, TopKRunMemoPtr memo, Block candidate_keys);

}
