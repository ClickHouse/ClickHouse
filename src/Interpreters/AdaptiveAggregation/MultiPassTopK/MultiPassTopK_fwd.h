#pragma once

#include <memory>

namespace DB
{

/// What a multi-pass top-K aggregation takes from the plan: the top of the query, and the input that it reads again for
/// each of its passes (see `makeMultiPassTopKInput`).
struct MultiPassTopKInput;
using MultiPassTopKInputPtr = std::shared_ptr<const MultiPassTopKInput>;

/// The candidates of the candidate pass of a multi-pass top-K aggregation, and one stream of the pass (see
/// `TopKCandidates`).
struct TopKCandidates;
using TopKCandidatesPtr = std::shared_ptr<TopKCandidates>;
class TopKCandidateStream;

}
