#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/Fragments/PreparedFragmentGraph.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>

#include <fmt/format.h>

namespace DB
{

void explainAsFragments(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length)
{
    const PreparedFragmentGraph prepared(graph, options);

    if (prepared.single_node)
    {
        writeSingleNode(prepared, buf, max_step_description_length);
        return;
    }

    for (const auto * fragment : prepared.print_order)
    {
        buf << fmt::format("Query fragment {} ({}) · tasks={}\n", fragment->id, fragment->stage_name, fragment->tasks);
        PreparedFragmentGraph::getPlan(*fragment).explainPlan(
            buf, options, 0, max_step_description_length, options.pretty ? &prepared.pretty_names : nullptr);
        buf << '\n';
    }

    writeExchangesSection(prepared, buf);
    writeLevelsSection(graph, buf);
}

}
