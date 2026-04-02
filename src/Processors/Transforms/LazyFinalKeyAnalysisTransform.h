#pragma once

#include <Core/Block.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Waits for the set to be built (input signal from CreatingSetsTransform),
/// then checks if the set was truncated. Pushes a chunk if the set is OK
/// (signaling InputSelectorTransform to use the optimized path),
/// or finishes without pushing (signaling fallback).
class LazyFinalKeyAnalysisTransform : public IProcessor
{
public:
    explicit LazyFinalKeyAnalysisTransform(FutureSetPtr future_set_);

    String getName() const override { return "LazyFinalKeyAnalysisTransform"; }
    Status prepare() override;

private:
    FutureSetPtr future_set;
};

}
