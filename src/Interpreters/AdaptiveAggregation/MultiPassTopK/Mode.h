#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Memo.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopKImpl.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Logger.h>

#include <memory>
#include <optional>

/// The interface of the modes of a multi-pass top-K aggregation, whose passes the controller runs (see
/// `addMultiPassTopK`).

namespace DB::MultiPassTopKImpl
{

struct TopKNext;

/// A mode of a multi-pass top-K aggregation, or the pilot that chooses one: the passes that it runs over the input, one
/// at a time, and what it concludes from the output of each. The controller runs the passes and emits the result (see
/// `MultiPassTopKController`).
class TopKMode
{
public:
    TopKMode(MultiPassTopKTemplatePtr templ_, TopKRunMemoPtr memo_)
        : templ(std::move(templ_))
        , memo(std::move(memo_))
        , log(getLogger("MultiPassTopK"))
    {
    }

    virtual ~TopKMode() = default;

    /// The plan of the first pass. Called once, before `afterPass`.
    virtual QueryPlan firstPass() = 0;

    /// What follows the pass that finished, whose output is `output`.
    virtual TopKNext afterPass(Block output) = 0;

protected:
    const MultiPassTopKTemplatePtr templ;
    /// What the run started from, and what it keeps for the next runs.
    const TopKRunMemoPtr memo;
    const LoggerPtr log;
};

using TopKModePtr = std::unique_ptr<TopKMode>;

/// What follows a pass (see `TopKMode::afterPass`): the groups of the result that the pass found, and then the next
/// pass of the mode, the first pass of another mode, or a decline. With none of them the run emits its result.
struct TopKNext
{
    Chunks result = {};
    std::optional<QueryPlan> next_pass = {};
    TopKModePtr next_mode = {};
    /// Why the run declines: the original aggregation runs instead.
    const char * decline = nullptr;
};

}
