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
    writeDependenciesSection(graph, buf);

    const auto levels = computeLevels(graph);
    buf << "Levels: ";
    for (size_t i = 0; i < levels.size(); ++i)
    {
        if (i != 0)
            buf << " -> ";
        std::vector<String> names;
        names.reserve(levels[i].size());
        for (UInt32 id : levels[i])
            names.push_back(fmt::format("F{}", id));
        buf << fmt::format("[{}]", fmt::join(names, ", "));
    }
    buf << '\n';
}

}
