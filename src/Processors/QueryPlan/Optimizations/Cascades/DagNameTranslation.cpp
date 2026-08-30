#include <Processors/QueryPlan/Optimizations/Cascades/DagNameTranslation.h>

#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// `materialize(x)` preserves values and thus the hash, so it is transparent to hash-based
/// distribution and can be traced through. `VIEW` reads insert such nodes.
static bool isMaterializeNode(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::FUNCTION
        && node.children.size() == 1
        && node.function_base
        && node.function_base->getName() == "materialize";
}

TranslatedName classifyOutputName(const ActionsDAG & dag, const String & output_name, String & input_name)
{
    const ActionsDAG::Node * node = dag.tryFindInOutputs(output_name);
    if (!node)
        return TranslatedName::Passthrough;
    while (node->type == ActionsDAG::ActionType::ALIAS || isMaterializeNode(*node))
        node = node->children.front();
    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        input_name = node->result_name;
        return TranslatedName::Traced;
    }
    return TranslatedName::Computed;
}

bool translateDistributionColumns(const ActionsDAG & dag, std::vector<NameSet> & columns)
{
    for (auto & column_set : columns)
    {
        NameSet translated;
        for (const auto & name : column_set)
        {
            String input_name;
            switch (classifyOutputName(dag, name, input_name))
            {
                case TranslatedName::Traced:
                    translated.insert(input_name);
                    break;
                case TranslatedName::Passthrough:
                    translated.insert(name);
                    break;
                case TranslatedName::Computed:
                    break;
            }
        }
        if (translated.empty())
            return false;
        column_set = std::move(translated);
    }
    return true;
}

bool translateSortDescription(const ActionsDAG & dag, SortDescription & sort_desc)
{
    for (auto & col_desc : sort_desc)
    {
        String input_name;
        switch (classifyOutputName(dag, col_desc.column_name, input_name))
        {
            case TranslatedName::Traced:
                col_desc.column_name = input_name;
                break;
            case TranslatedName::Passthrough:
                break;
            case TranslatedName::Computed:
                return false;
        }
    }
    return true;
}

}
