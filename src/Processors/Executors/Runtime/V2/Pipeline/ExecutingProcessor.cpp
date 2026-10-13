#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>

namespace DB::Runtime::V2
{

struct ProcessorUniqID
{
    const IProcessor & processor;
};

}

template <>
struct fmt::formatter<DB::Runtime::V2::ProcessorUniqID> : fmt::formatter<std::string>
{
    auto format(const DB::Runtime::V2::ProcessorUniqID & id, format_context & ctx) const
    {
        return fmt::formatter<std::string>::format(id.processor.getUniqID(), ctx);
    }
};

namespace DB
{

namespace Runtime::V2
{

void ExecutingProcessor::setProcessor(IProcessor & processor_)
{
    processor = &processor_;
    Wyrd::setName(round_mutex, "processor[{}].round_mutex", ProcessorUniqID{processor_});
    Wyrd::setName(status, "processor[{}].status", ProcessorUniqID{processor_});
}

std::unique_lock<Wyrd::Mutex> ExecutingProcessor::lockRound()
{
    return std::unique_lock(round_mutex);
}

}

}
