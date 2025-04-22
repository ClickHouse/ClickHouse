#pragma once

#include "Hypothesis.h"

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <base/StringRef.h>

namespace DB::Hypothesis
{

class Deducer
{
public:
    explicit Deducer(Block block_);

    HypothesisVec deduceColumn(std::string_view name);


private:
    Block block;
    LoggerPtr log = nullptr;
};

}
