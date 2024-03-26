#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/CaseWhenSimplifyPass.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/equals.h>
#include <Functions/notEquals.h>

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

QueryTreeNodePtr
combineNodesWithFunction(const FunctionOverloadResolverPtr & function_resolver, const std::vector<QueryTreeNodePtr> & arguments)
{
    if (arguments.size() > 1)
    {
        QueryTreeNodePtr current = arguments[0];
        for (size_t i = 1; i < arguments.size(); ++i)
            current = createFunctionNode(function_resolver, std::move(current), arguments[i]);
        return current;
    }
    else
        return arguments[0];
}

std::vector<size_t> findPosition(const std::vector<ConstantNode *> & values, const ConstantNode & value)
{
    std::vector<size_t> positions;
    for (size_t i = 0; i < values.size(); ++i)
        if (values[i]->getValue() == value.getValue())
            positions.push_back(i);
    return positions;
}

class CaseWhenSimplifyPassVisitor : public InDepthQueryTreeVisitorWithContext<CaseWhenSimplifyPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CaseWhenSimplifyPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        // if parent has isNull or isNotNull function, abandon the optimization
        if (parentHasIsNull)
        {
            return;
        }
        static const std::unordered_set<String> is_null_funcs = {"isNull", "isNotNull"};
        static const std::unordered_set<String> supported_funcs = {"in", "notIn", "equals", "notEquals"};
        auto * func_node = node->as<FunctionNode>();
        if (!func_node)
            return;
        if (is_null_funcs.contains(func_node->getFunctionName()))
        {
            parentHasIsNull = true;
            return;
        }

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
        bool values_contain_null = false;
        bool keys_contain_null = false;
        Tuple keys_which_value_is_null;
        Tuple keys_which_value_not_null;
        size_t value_null_num = 0;
        // extract keys and values from case
        for (size_t i = 1; i < case_args.size() - (has_else ? 1 : 0); i += 2)
        {
            auto * key = case_args[i]->as<ConstantNode>();
            auto * value = case_args[i + 1]->as<ConstantNode>();
            if (!key || !value)
                return;
            if (key->getValue().isNull())
            {
                keys_contain_null = true;
            }
            keys.push_back(key);
            values.push_back(value);
            if (value->getValue().isNull())
            {
                keys_which_value_is_null.push_back(key->getValue());
                values_contain_null = true;
                value_null_num++;
            }
            else
            {
                keys_which_value_not_null.push_back(key->getValue());
            }
        }
        if (has_else)
        {
            auto * else_node = case_args.back()->as<ConstantNode>();
            if (else_node->getValue().isNull())
            {
                // else node is null, remove it
                has_else = false;
            }
            else
            {
                values.push_back(else_node);
            }
        }

        bool values_are_all_null = value_null_num == values.size();
        auto is_else_value_found
            = [&](std::vector<size_t> pos_list) { return has_else && pos_list.size() == 1 && pos_list.front() == values.size() - 1; };
        auto value_node_not_exists = [&](std::vector<size_t> pos_list) { return pos_list.empty(); };
        if (func_node->getFunctionName() == "equals")
        {
            auto pos_list = findPosition(values, *value_node);
            // (case x when 'a' then 1 when 'b' then 2 else 3 end) = 4 => False
            // (case x when 'a' then 1 when 'b' then 2 else 3 end) = Null => False
            if (value_node_not_exists(pos_list) || value_node->getValue().isNull())
                node = std::make_shared<ConstantNode>(Field(false));
            else if (is_else_value_found(pos_list))
            {
                // (case x when 'a' then 1 when 'b' then 2 else 3 end) = 3 => x notIn ('a', 'b')
                Tuple tuple;
                for (const auto * key : keys)
                    tuple.emplace_back(key->getValue());
                auto tuple_node = std::make_shared<ConstantNode>(Field(tuple));
                auto not_in_function_resolver = FunctionFactory::instance().get("notIn", getContext());
                node = createFunctionNode(not_in_function_resolver, case_column->clone(), tuple_node);
                if (!keys_contain_null)
                {
                    // (case x when NULL then 1 when 'b' then 2 else 3 end) = 3 => x notIn (NULL, 'b') or x is null
                    auto is_null_function_resolver = FunctionFactory::instance().get("isNull", getContext());
                    auto is_null_node = createFunctionNode(is_null_function_resolver, case_column->clone());
                    node = createFunctionNode(FunctionFactory::instance().get("or", getContext()), node, is_null_node);
                }
            }
            else
            {
                // (case x when 'a' then 2 when 'b' then 2 else 3 end) = 2 => x = 'a' or x = 'b'
                auto equals_resolver = createInternalFunctionEqualOverloadResolver(getContext()->getSettingsRef().decimal_check_overflow);
                auto is_null_function_resolver = FunctionFactory::instance().get("isNull", getContext());
                std::vector<QueryTreeNodePtr> equals_nodes;
                for (size_t pos : pos_list)
                {
                    if (keys[pos]->getValue().isNull())
                        equals_nodes.emplace_back(createFunctionNode(is_null_function_resolver, case_column->clone()));
                    else
                        equals_nodes.emplace_back(createFunctionNode(equals_resolver, case_column->clone(), keys.at(pos)->clone()));
                }
                node = combineNodesWithFunction(FunctionFactory::instance().get("or", getContext()), equals_nodes);
            }
        }

        if (func_node->getFunctionName() == "notEquals")
        {
            auto pos_list = findPosition(values, *value_node);
            // case x when 'a' then 1 when 'b' then 2 != null => False
            // case x when 'a' then null when 'b' then null != null => False
            if (value_node->getValue().isNull() || values_are_all_null)
                node = std::make_shared<ConstantNode>(Field(false));
            // case x when 'a' then 1 when 'b' then 2 else 4 != 3 => True
            else if (!values_contain_null && value_node_not_exists(pos_list) && has_else)
                node = std::make_shared<ConstantNode>(Field(true));
            else if (!values_contain_null && value_node_not_exists(pos_list) && !has_else)
            {
                auto not_in_function_resolver = FunctionFactory::instance().get("in", getContext());
                node = createFunctionNode(
                    not_in_function_resolver, case_column->clone(), std::make_shared<ConstantNode>(Field(keys_which_value_not_null)));
            }
            // case x when 'a' then Null when 'b' then Null when 'c' then 3 != 4 => x notIn ('a', 'b')
            else if (values_contain_null && value_node_not_exists(pos_list))
            {
                auto not_in_function_resolver = FunctionFactory::instance().get("notIn", getContext());
                node = createFunctionNode(
                    not_in_function_resolver, case_column->clone(), std::make_shared<ConstantNode>(Field(keys_which_value_is_null)));
            }
            // case x when 'a' then 1 when 'b' then 2 else 3 != 3 => x in ('a', 'b')
            else if (is_else_value_found(pos_list))
            {
                Tuple tuple;
                for (const auto * key : keys)
                    tuple.emplace_back(key->getValue());
                auto tuple_node = std::make_shared<ConstantNode>(Field(tuple));
                auto in_function_resolver = FunctionFactory::instance().get("in", getContext());
                node = createFunctionNode(in_function_resolver, case_column->clone(), tuple_node);
            }
            else if (!has_else)
            {
                Tuple valid_value;
                for (size_t i = 0; i < keys.size(); i++)
                {
                    if (std::find(pos_list.begin(), pos_list.end(), i) == pos_list.end())
                        valid_value.push_back(keys[i]->getValue());
                }
                auto in_function_resolver = FunctionFactory::instance().get("in", getContext());
                node = createFunctionNode(in_function_resolver, case_column->clone(), std::make_shared<ConstantNode>(Field(valid_value)));
            }
            else
            {
                // (case x when 'a' then 1 when 'b' then 2 else 3 end) <> 1 => x != 'a' or x is null
                auto not_equals_resolver
                    = createInternalFunctionNotEqualOverloadResolver(getContext()->getSettingsRef().decimal_check_overflow);
                auto is_null_function_resolver = FunctionFactory::instance().get("isNull", getContext());
                auto is_not_null_function_resolver = FunctionFactory::instance().get("isNotNull", getContext());
                std::vector<QueryTreeNodePtr> result_nodes;
                bool column_is_not_null = false;
                for (size_t pos : pos_list)
                {
                    if (keys[pos]->getValue().isNull())
                    {
                        column_is_not_null = true;
                        result_nodes.emplace_back(createFunctionNode(is_not_null_function_resolver, case_column->clone()));
                    }
                    else
                        result_nodes.emplace_back(createFunctionNode(not_equals_resolver, case_column->clone(), keys.at(pos)->clone()));
                }
                node = combineNodesWithFunction(FunctionFactory::instance().get("and", getContext()), result_nodes);
                if (!column_is_not_null)
                {
                    auto is_null_node = createFunctionNode(is_null_function_resolver, case_column->clone());
                    node = createFunctionNode(FunctionFactory::instance().get("or", getContext()), node, is_null_node);
                }
            }
        }

        if (func_node->getFunctionName() == "in")
        {
            Tuple in_values;

            if (value_node->getValue().getType() != Field::Types::Tuple && value_node->getValue().getType() != Field::Types::Array)
            {
                if (!value_node->getValue().isNull())
                    in_values.push_back(value_node->getValue());
            }
            else if (value_node->getValue().getType() == Field::Types::Tuple)
            {
                for (const auto & field : value_node->getValue().get<Tuple>())
                    if (!field.isNull())
                        in_values.push_back(field);
            }
            else if (value_node->getValue().getType() == Field::Types::Array)
            {
                for (const auto & field : value_node->getValue().get<Array>())
                    if (field.isNull())
                        in_values.push_back(field);
            }
            else
                return;
            Tuple in_keys;
            Tuple not_in_keys;
            bool has_not_in = false;
            for (const auto & value : in_values)
            {
                auto pos_list = findPosition(values, ConstantNode(value));
                if (pos_list.empty())
                    continue;
                if (is_else_value_found(pos_list))
                {
                    has_not_in = true;
                    continue;
                }
                for (size_t pos : pos_list)
                    in_keys.push_back(keys.at(pos)->getValue());
            }
            if (has_not_in)
            {
                for (const auto & key : keys)
                    if (std::find(in_keys.begin(), in_keys.end(), key->getValue()) == in_keys.end())
                        not_in_keys.push_back(key->getValue());
            }
            QueryTreeNodePtr in_node, not_in_node;
            if (!in_keys.empty())
            {
                auto in_function_resolver = FunctionFactory::instance().get("in", getContext());
                auto in_keys_node = std::make_shared<ConstantNode>(Field(in_keys));
                in_node = createFunctionNode(in_function_resolver, case_column->clone(), in_keys_node);
            }
            if (!not_in_keys.empty())
            {
                auto not_in_function_resolver = FunctionFactory::instance().get("notIn", getContext());
                auto not_in_keys_node = std::make_shared<ConstantNode>(Field(not_in_keys));
                not_in_node = createFunctionNode(not_in_function_resolver, case_column->clone(), not_in_keys_node);
            }
            if (!in_keys.empty() && !not_in_keys.empty())
            {
                auto or_function_resolver = FunctionFactory::instance().get("or", getContext());
                node = createFunctionNode(or_function_resolver, in_node, not_in_node);
            }
            else if (!in_keys.empty())
                // (case x when 'a' then 1 when 'b' then 2 else 3 end) in (1,2) => x in ('a', 'b')
                node = in_node;
            else if (!not_in_keys.empty())
            {
                node = not_in_node;
                auto is_null_function_resolver = FunctionFactory::instance().get("isNull", getContext());
                auto is_null_node = createFunctionNode(is_null_function_resolver, case_column->clone());
                node = createFunctionNode(FunctionFactory::instance().get("or", getContext()), node, is_null_node);
            }
            else
                node = std::make_shared<ConstantNode>(Field(false));
        }

        if (func_node->getFunctionName() == "notIn")
        {
            Tuple not_in_values;
            if (value_node->getValue().getType() != Field::Types::Tuple && value_node->getValue().getType() != Field::Types::Array)
                not_in_values.push_back(value_node->getValue());
            else if (value_node->getValue().getType() == Field::Types::Tuple)
                for (const auto & field : value_node->getValue().get<Tuple>())
                    not_in_values.push_back(field);
            else if (value_node->getValue().getType() == Field::Types::Array)
                for (const auto & field : value_node->getValue().get<Array>())
                    not_in_values.push_back(field);
            else
                return;

            Tuple in_keys;
            Tuple not_in_keys;
            bool has_in = false;
            for (const auto & value : not_in_values)
            {
                auto pos_list = findPosition(values, ConstantNode(value));
                if (pos_list.empty())
                    continue;
                if (is_else_value_found(pos_list))
                {
                    has_in = true;
                    continue;
                }
                for (size_t pos : pos_list)
                    not_in_keys.push_back(keys.at(pos)->getValue());
            }
            if (has_in)
            {
                for (const auto & key : keys)
                    if (std::find(not_in_keys.begin(), not_in_keys.end(), key->getValue()) == not_in_keys.end())
                        in_keys.push_back(key->getValue());
            }
            QueryTreeNodePtr in_node, not_in_node;
            if (!in_keys.empty())
            {
                auto in_function_resolver = FunctionFactory::instance().get("in", getContext());
                auto in_keys_node = std::make_shared<ConstantNode>(Field(in_keys));
                in_node = createFunctionNode(in_function_resolver, case_column->clone(), in_keys_node);
            }
            if (!not_in_keys.empty())
            {
                auto not_in_function_resolver = FunctionFactory::instance().get("notIn", getContext());
                auto not_in_keys_node = std::make_shared<ConstantNode>(Field(not_in_keys));
                not_in_node = createFunctionNode(not_in_function_resolver, case_column->clone(), not_in_keys_node);
            }
            if (!in_keys.empty() && !not_in_keys.empty())
            {
                auto and_function_resolver = FunctionFactory::instance().get("and", getContext());
                node = createFunctionNode(and_function_resolver, in_node, not_in_node);
            }
            else if (!not_in_keys.empty())
            {
                node = not_in_node;
                auto is_null_function_resolver = FunctionFactory::instance().get("isNull", getContext());
                auto is_null_node = createFunctionNode(is_null_function_resolver, case_column->clone());
                node = createFunctionNode(FunctionFactory::instance().get("or", getContext()), node, is_null_node);
            }
            else if (!in_keys.empty())
                node = in_node;
            else
                node = std::make_shared<ConstantNode>(Field(true));
        }
    }

private:
    bool parentHasIsNull = false;
};
}

void CaseWhenSimplifyPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    CaseWhenSimplifyPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}
}
