#include <Analyzer/Utils.h>

namespace DB
{

bool isNodePartOfTree(const IQueryTreeNode * node, const IQueryTreeNode * root)
{
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        const auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (subtree_node == node)
            return true;

        for (const auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return false;
}

bool isNameOfInFunction(const std::string & function_name)
{
    bool is_special_function_in = function_name == "in" || function_name == "globalIn" || function_name == "notIn" || function_name == "globalNotIn" ||
        function_name == "nullIn" || function_name == "globalNullIn" || function_name == "notNullIn" || function_name == "globalNotNullIn" ||
        function_name == "inIgnoreSet" || function_name == "globalInIgnoreSet" || function_name == "notInIgnoreSet" || function_name == "globalNotInIgnoreSet" ||
        function_name == "nullInIgnoreSet" || function_name == "globalNullInIgnoreSet" || function_name == "notNullInIgnoreSet" || function_name == "globalNotNullInIgnoreSet";

    return is_special_function_in;
}

}
