#include <Interpreters/PredicateAtomExtractor.h>
#include <Functions/IFunction.h>


namespace DB
{

String classifyPredicateFunction(const String & function_name)
{
    if (function_name == "equals" || function_name == "notEquals")
        return "Equality";

    if (function_name == "less" || function_name == "greater"
        || function_name == "lessOrEquals" || function_name == "greaterOrEquals")
        return "Range";

    if (function_name == "in" || function_name == "globalIn"
        || function_name == "notIn" || function_name == "globalNotIn")
        return "In";

    if (function_name == "like" || function_name == "ilike"
        || function_name == "notLike" || function_name == "notILike")
        return "LikeSubstring";

    if (function_name == "isNull" || function_name == "isNotNull")
        return "IsNull";

    return "Other";
}

namespace
{

String findInputColumnName(const ActionsDAG::Node * node)
{
    if (!node)
        return {};

    if (node->type == ActionsDAG::ActionType::INPUT)
        return node->result_name;

    if (node->type == ActionsDAG::ActionType::ALIAS)
    {
        if (!node->children.empty())
            return findInputColumnName(node->children[0]);
        return {};
    }

    /// function nodes, check children for INPUT nodes
    for (const auto * child : node->children)
    {
        if (child->type == ActionsDAG::ActionType::INPUT)
            return child->result_name;
    }

    for (const auto * child : node->children)
    {
        String result = findInputColumnName(child);
        if (!result.empty())
            return result;
    }

    return {};
}

}

std::vector<PredicateAtom> extractPredicateAtoms(const ActionsDAG::Node * filter_node)
{
    std::vector<PredicateAtom> result;

    if (!filter_node)
        return result;

    auto atoms = ActionsDAG::extractConjunctionAtoms(filter_node);

    for (const auto * atom_node : atoms)
    {
        if (atom_node->type != ActionsDAG::ActionType::FUNCTION || !atom_node->function_base)
            continue;

        String func_name = atom_node->function_base->getName();
        String column_name = findInputColumnName(atom_node);

        /// skip atoms where we cannot determine the column
        if (column_name.empty())
            continue;

        PredicateAtom atom;
        atom.column_name = std::move(column_name);
        atom.function_name = func_name;
        atom.predicate_class = classifyPredicateFunction(func_name);
        result.push_back(std::move(atom));
    }

    return result;
}

}
