#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>

namespace DB
{

std::pair<QueryPlanStepPtr, QueryPlanStepPtr> GatherExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    auto sink = std::make_unique<GatherSendStep>(input_headers.front(), exchange_id);

    auto source = std::make_unique<GatherReceiveStep>(output_header.value(), exchange_id, source_shards.size(), maintain_sort_description);

    return {std::move(sink), std::move(source)};
}

}
