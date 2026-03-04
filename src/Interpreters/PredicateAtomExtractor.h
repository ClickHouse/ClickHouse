#pragma once

#include <Interpreters/ActionsDAG.h>
#include <base/types.h>
#include <vector>


namespace DB
{

struct PredicateAtom
{
    String column_name;
    String predicate_class;   /// "Equality", "Range", "In", "LikeSubstring", "IsNull", "Other"
    String function_name;     /// "equals", "less", ...
};

/// uses ActionsDAG::extractConjunctionAtoms to decompose AND chains
/// then classifies each atom by inspecting `function_base->getName`
std::vector<PredicateAtom> extractPredicateAtoms(const ActionsDAG::Node * filter_node);

/// classify function name into a predicate_class
String classifyPredicateFunction(const String & function_name);

}
