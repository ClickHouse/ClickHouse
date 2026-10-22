#include <Storages/MergeTree/WhatIfFilterAnalysis.h>

#include <Functions/IFunction.h>

namespace DB
{

void collectFilterInputColumns(const ActionsDAG::Node * node, NameSet & out)
{
    if (!node)
        return;
    if (node->type == ActionsDAG::ActionType::INPUT)
        out.insert(node->result_name);
    for (const auto * child : node->children)
        collectFilterInputColumns(child, out);
}

bool disjunctionMixesIndexAndOtherColumns(const ActionsDAG::Node * node, const NameSet & index_columns)
{
    if (!node)
        return false;

    if (node->type == ActionsDAG::ActionType::FUNCTION
        && node->function_base
        && node->function_base->getName() == "or")
    {
        bool branch_has_index = false;
        bool branch_has_other = false;
        for (const auto * child : node->children)
        {
            NameSet cols;
            collectFilterInputColumns(child, cols);
            for (const auto & col : cols)
            {
                if (index_columns.contains(col))
                    branch_has_index = true;
                else
                    branch_has_other = true;
            }
        }
        if (branch_has_index && branch_has_other)
            return true;
    }

    for (const auto * child : node->children)
        if (disjunctionMixesIndexAndOtherColumns(child, index_columns))
            return true;
    return false;
}

}
