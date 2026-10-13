#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/IProcessor.h>

#include <memory>

namespace DB::Runtime::Bench
{

struct Load
{
    size_t chunks = 150;
    size_t rows_per_chunk = 65409;
    bool interleaved = false;
};

std::shared_ptr<Processors> chain(ContextPtr context, Load load);
std::shared_ptr<Processors> fanOut(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> fanIn(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> hub(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> fanInMerge(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> manySources(ContextPtr context, size_t sources, size_t threads, Load load);
std::shared_ptr<Processors> delayed(ContextPtr context, Load load);
std::shared_ptr<Processors> fanInAggregate(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> aggregate(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> manyPartsInOrder(ContextPtr context, size_t parts, size_t streams, Load load);

}
