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
};

std::shared_ptr<Processors> chain(ContextPtr context, Load load);
std::shared_ptr<Processors> fanOut(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> fanIn(ContextPtr context, size_t streams, Load load);
std::shared_ptr<Processors> hub(ContextPtr context, size_t streams, Load load);

}
