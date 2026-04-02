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
class SetReadinessSignalTransform : public IProcessor
{
public:
    explicit SetReadinessSignalTransform(FutureSetPtr future_set_);

    String getName() const override { return "SetReadinessSignalTransform"; }
    Status prepare() override;

private:
    FutureSetPtr future_set;
};

}
