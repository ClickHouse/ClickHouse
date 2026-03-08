#include <Interpreters/PredicateAtomExtractor.h>
#include <Functions/IFunction.h>
#include <unordered_set>


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

void collectInputColumnNames(const ActionsDAG::Node * node, std::unordered_set<String> & out)
{
    if (!node)
        return;

    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        out.insert(node->result_name);
        return;
    }

    for (const auto * child : node->children)
        collectInputColumnNames(child, out);
}

/// return single INPUT column name if the subtree references exactly one column
/// for multi-column predicates (`a > b`, `a + b > 0`) return empty
String findSingleInputColumnName(const ActionsDAG::Node * node)
{
    std::unordered_set<String> columns;
    collectInputColumnNames(node, columns);
    if (columns.size() == 1)
        return *columns.begin();
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
        String column_name = findSingleInputColumnName(atom_node);

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
