#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/Fragments/FragmentGraphFormat.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void explainFragmentGraph(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length)
{
    if (graph.fragments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The fragment graph has no fragments");

    /// The plan was not cut: print it as `EXPLAIN` without `fragments` does.
    if (tryExplainSingleFragment(graph, buf, options, max_step_description_length))
        return;

    for (const auto * fragment : getFragmentPrintOrder(graph))
    {
        if (!fragment->plan)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to explain", fragment->id);
        buf << fmt::format("Query fragment {} ({}) · tasks={}\n", fragment->id, fragment->stage_name, fragment->tasks);
        fragment->plan->explainPlan(buf, options, 0, max_step_description_length);
        buf << '\n';
    }

    writeExchangesSection(graph, buf, options.pretty);
    writeLevelsSection(graph, buf);
}

}
