#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/// Returns true if node part of root tree, false otherwise
bool isNodePartOfTree(const IQueryTreeNode * node, const IQueryTreeNode * root);

/// Returns true if function name is name of IN function or its variations, false otherwise
bool isNameOfInFunction(const std::string & function_name);

}
