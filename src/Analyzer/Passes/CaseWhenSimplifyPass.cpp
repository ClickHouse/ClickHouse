#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/CaseWhenSimplifyPass.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
bool checkFunctionWithArguments(QueryTreeNodePtr node, const std::unordered_set<String> & func_names, size_t num_args)
{
    auto * func_node = node->as<FunctionNode>();
    bool not_passed
        = !func_node || !func_names.contains(func_node->getFunctionName()) || func_node->getArguments().getNodes().size() != num_args;
    return !not_passed;
}

template <typename LEFT, typename RIGHT>
bool checkFunctionArgumentsType(const FunctionNode * node)
{
    auto args = node->getArguments().getNodes();
    auto * left = args[0]->as<LEFT>();
    auto * right = args[1]->as<RIGHT>();
    if (!left || !right)
        return false;
    return true;
}

template <typename... Args>
QueryTreeNodePtr createFunctionNode(const FunctionOverloadResolverPtr & function_resolver, Args &&... args)
{
    auto function_node = std::make_shared<FunctionNode>(function_resolver->getName());
    auto & new_arguments = function_node->getArguments().getNodes();
    new_arguments.reserve(sizeof...(args));
    (new_arguments.push_back(std::forward<Args>(args)), ...);
    function_node->resolveAsFunction(function_resolver);
    return function_node;
}

size_t findPosition(const std::vector<ConstantNode *> & values, const ConstantNode & value)
{
    for (size_t i = 0; i < values.size(); ++i)
        if (values[i]->getValue() == value.getValue())
            return i;
    return values.size();
}

class CaseWhenSimplifyPassVisitor : public InDepthQueryTreeVisitorWithContext<CaseWhenSimplifyPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CaseWhenSimplifyPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        static const std::unordered_set<String> supported_funcs = {"in", "notIn", "equals", "notEquals"};
        auto * func_node = node->as<FunctionNode>();
        if (!checkFunctionWithArguments(node, supported_funcs, 2))
            return;
        if (!checkFunctionArgumentsType<FunctionNode, ConstantNode>(func_node))
            return;
        auto * case_node = func_node->getArguments().getNodes()[0]->as<FunctionNode>();
        auto * value_node = func_node->getArguments().getNodes()[1]->as<ConstantNode>();
        if (case_node->getFunctionName() != "caseWithExpression")
            return;

        auto case_args = case_node->getArguments().getNodes();
        auto * case_column = case_args[0]->as<ColumnNode>();
        bool has_else = (case_args.size() - 1) % 2 == 1;
        std::vector<ConstantNode *> keys;
        std::vector<ConstantNode *> values;
        bool values_has_null = false;
        Tuple keys_which_value_is_null;
        size_t value_null_num = 0;
        // extract keys and values from case
        for (size_t i = 1; i < case_args.size() - (has_else ? 1 : 0); i += 2)
        {
            auto * key = case_args[i]->as<ConstantNode>();
            auto * value = case_args[i + 1]->as<ConstantNode>();
            if (!key || !value)
                return;
            keys.push_back(key);
            values.push_back(value);
            if (value->getValue().isNull())
            {
                keys_which_value_is_null.push_back(key->getValue());
                values_has_null = true;
                value_null_num++;
            }
        }
        if (has_else)
        {
            values.push_back(case_args.back()->as<ConstantNode>());
            if (values.back()->getValue().isNull())
                value_null_num++;
        }
        bool values_are_all_null = value_null_num == values.size();

        if (bool is_equals = func_node->getFunctionName() == "equals")
        {
            auto pos = findPosition(values, *value_node);
            if (pos == values.size() || value_node->getValue().isNull())
                // value not found in case values, always false
                node = std::make_shared<ConstantNode>(Field(false));
            else if (has_else && pos == values.size() - 1)
            {
                // value is in else, replace equals with not in
                Tuple tuple;
                for (const auto * key : keys)
                    tuple.emplace_back(key->getValue());
                auto tuple_node = std::make_shared<ConstantNode>(Field(tuple));
                auto not_in_function_resolver = FunctionFactory::instance().get("notIn", getContext());
                node = createFunctionNode(not_in_function_resolver, case_column->clone(), tuple_node);
            }
            else
                // replace case when with simple equals
                func_node->getArguments().getNodes() = {case_column->clone(), keys.at(pos)->clone()};
        }

        if (bool is_not_equals = func_node->getFunctionName() == "notEquals")
        {
            auto pos = findPosition(values, *value_node);
            // case x when 'a' then 1 when 'b' then 2 != null => False
            // case x when 'a' then null when 'b' then null != null => False
            if (value_node->getValue().isNull() || values_are_all_null)
                node = std::make_shared<ConstantNode>(Field(false));
            // case x when 'a' then 1 when 'b' then 2 != 3 => True
            else if (!values_has_null && pos == values.size())
                node = std::make_shared<ConstantNode>(Field(true));
            // case x when 'a' then Null when 'b' then Null when 'c' then 3 != 4 => x notIn ('a', 'b')
            else if (values_has_null && pos == values.size())
            {
                auto not_in_function_resolver = FunctionFactory::instance().get("notIn", getContext());
                node = createFunctionNode(
                    not_in_function_resolver, case_column->clone(), std::make_shared<ConstantNode>(Field(keys_which_value_is_null)));
            }
            // case x when 'a' then 1 when 'b' then 2 else 3 != 3 => x in ('a', 'b')
            else if (has_else && pos == values.size() - 1)
            {
                Tuple tuple;
                for (const auto * key : keys)
                    tuple.emplace_back(key->getValue());
                auto tuple_node = std::make_shared<ConstantNode>(Field(tuple));
                auto not_in_function_resolver = FunctionFactory::instance().get("in", getContext());
                node = createFunctionNode(not_in_function_resolver, case_column->clone(), tuple_node);
            }
            // case x when 'a' then 1 when 'b' then 2  != 2 => x != 'b'
            else
                func_node->getArguments().getNodes() = {case_column->clone(), keys.at(pos)->clone()};
        }

        if (func_node->getFunctionName() == "in")
        {
            Tuple in_values;
            if (value_node->getValue().getType() != Field::Types::Tuple && value_node->getValue().getType() != Field::Types::Array)
                in_values.push_back(value_node->getValue());
            else if (value_node->getValue().getType() == Field::Types::Tuple)
            {
                for (const auto & field : value_node->getValue().get<Tuple>())
                {
                    in_values.push_back(field);
                }
            }
            else if (value_node->getValue().getType() == Field::Types::Array)
            {
                for (const auto & field : value_node->getValue().get<Array>())
                {
                    in_values.push_back(field);
                }
            }

            Tuple in_keys;
            Tuple not_in_keys;
            for (const auto & value : in_values)
            {
                auto pos = findPosition(values, ConstantNode(value));
                if (pos == values.size()) continue;
                if ()
                in_keys.push_back(keys.at(pos)->getValue());
            }

        }
    }
};
}

void CaseWhenSimplifyPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    CaseWhenSimplifyPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}
}
