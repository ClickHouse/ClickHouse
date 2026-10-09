#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>

#include <fmt/format.h>

namespace DB
{

namespace Runtime::V2
{

void ExecutingProcessor::setName(std::string name)
{
    Wyrd::setName(round_mutex, fmt::format("{}.round_mutex", name));
    Wyrd::setName(status, fmt::format("{}.status", name));
}

std::unique_lock<Wyrd::Mutex> ExecutingProcessor::lockRound()
{
    return std::unique_lock(round_mutex);
}

}

}
