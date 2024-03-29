#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/CaseWhenSimplifyPass.h>
#include <Analyzer/createUniqueTableAliases.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/equals.h>
#include <Functions/notEquals.h>

namespace DB
{
namespace
{
bool checkFunctionWithArguments(const QueryTreeNodePtr node, const std::unordered_set<String> & func_names, size_t num_args)
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


QueryTreeNodePtr equals(QueryTreeNodePtr left, const ConstantNode & right, ContextPtr context)
{
    if (right.getValue().isNull())
        return createFunctionNode(FunctionFactory::instance().get("isNull", context), std::move(left));
    return createFunctionNode(FunctionFactory::instance().get("equals", context), std::move(left), right.clone());
}

QueryTreeNodePtr notEquals(QueryTreeNodePtr left, const ConstantNode & right, ContextPtr context)
{
    if (right.getValue().isNull())
        return createFunctionNode(FunctionFactory::instance().get("isNotNull", context), std::move(left));
    return createFunctionNode(FunctionFactory::instance().get("notEquals", context), std::move(left), right.clone());
}

QueryTreeNodePtr
combineNodesWithFunction(const FunctionOverloadResolverPtr & function_resolver, const std::vector<QueryTreeNodePtr> & arguments, QueryTreeNodePtr default_result)
{
    if (arguments.empty())
        return default_result;
    if (arguments.size() > 1)
    {
        QueryTreeNodePtr current = arguments[0];
        for (size_t i = 1; i < arguments.size(); ++i)
            current = createFunctionNode(function_resolver, std::move(current), arguments[i]);
        return current;
    }
    return arguments[0];
}

class ICaseWhenReplaceAlogrithm
{
public:
    ICaseWhenReplaceAlogrithm(const FunctionNode & case_node, const ConstantNode & right_node, ContextPtr context_) : context(context_)
    {
        chassert(case_node.getFunctionName() == "caseWithExpression");
        auto case_args = case_node.getArguments().getNodes();
        if (case_args.size() < 3)
        {
            failed = true;
            return;
        }
        // invalid case function
        if (case_args.size() % 2 != 0)
        {
            failed = true;
            return;
        }
        has_else = (case_args.size() - 1) % 2 == 1;
        case_column = case_args.at(0);
        for (size_t i = 1; i < case_args.size() - (has_else ? 1 : 0); i += 2)
        {
            auto * key = case_args[i]->as<ConstantNode>();
            auto * value = case_args[i + 1]->as<ConstantNode>();
            if (!key || !value)
            {
                failed = true;
                return;
            }
            keys_contain_null = keys_contain_null || key->getValue().isNull();
            key_value_pairs.emplace_back(*key, *value);
        }
        if (has_else)
        {
            else_node = case_args.back()->as<ConstantNode>();
            if (!else_node)
            {
                failed = true;
                return;
            }
            if (else_node->getValue().isNull())
                has_else = false;
        }

        if (right_node.getValue().getType() != Field::Types::Tuple && right_node.getValue().getType() != Field::Types::Array)
        {
            if (!right_node.getValue().isNull())
                right_values.push_back(right_node.getValue());
        }
        else if (right_node.getValue().getType() == Field::Types::Tuple)
        {
            for (const auto & field : right_node.getValue().get<Tuple>())
                if (!field.isNull())
                    right_values.push_back(field);
        }
        else if (right_node.getValue().getType() == Field::Types::Array)
        {
            for (const auto & field : right_node.getValue().get<Array>())
                if (!field.isNull())
                    right_values.push_back(field);
        }
        size_t else_mapping_num = 0;
        for (const auto & right_value : right_values)
        {
            if (has_else && right_value == else_node->getValue())
            {
                right_value_map.emplace(right_value, else_node);
                else_mapping_num++;
            }
            else
            {
                for (const auto & key_value_pair : key_value_pairs)
                    if (right_value == key_value_pair.second.getValue())
                        right_value_map.emplace(right_value, &key_value_pair.first);
            }
        }
        only_else = else_mapping_num > 0 && else_mapping_num == right_value_map.size();
    }

    virtual ~ICaseWhenReplaceAlogrithm() = default;

    virtual bool support() { return !failed; }

    QueryTreeNodePtr replace()
    {
        if (!support())
            return nullptr;
        if (right_value_map.empty())
            return replaceImplNotFound();
        if (only_else)
            return replaceImplElse();
        if (!has_else)
            return replaceImplNoElse();
        return replaceImpl();
    }

protected:
    virtual QueryTreeNodePtr replaceImplNoElse() { return replaceImpl(); }
    virtual QueryTreeNodePtr replaceImplNotFound() { return replaceImpl(); }
    virtual QueryTreeNodePtr replaceImplElse() { return replaceImpl(); }
    virtual QueryTreeNodePtr replaceImpl() { return nullptr; }

    ContextPtr context;
    std::vector<std::pair<const ConstantNode &, const ConstantNode &>> key_value_pairs;
    const ConstantNode * else_node = nullptr;
    std::multimap<Field, const ConstantNode *> right_value_map;
    Tuple right_values;
    QueryTreeNodePtr case_column;
    bool has_else = false;
    bool only_else = false;
    bool keys_contain_null = false;
    bool failed = false;
};

class CaseWhenEqualsReplace : public ICaseWhenReplaceAlogrithm
{
public:
    CaseWhenEqualsReplace(const FunctionNode & case_node, const ConstantNode & right_node, ContextPtr context_)
        : ICaseWhenReplaceAlogrithm(case_node, right_node, context_)
    {
    }

protected:
    QueryTreeNodePtr replaceImplNotFound() override { return std::make_shared<ConstantNode>(Field(false)); }

    QueryTreeNodePtr replaceImplElse() override
    {
        if (keys_contain_null)
        {
            auto not_equal_resolver = createInternalFunctionNotEqualOverloadResolver(context->getSettings().decimal_check_overflow);
            std::vector<QueryTreeNodePtr> not_equals_conditions;
            for (const auto & key_value_pair : key_value_pairs)
                if (!key_value_pair.first.getValue().isNull())
                    not_equals_conditions.emplace_back(notEquals(case_column, key_value_pair.first, context));
            return combineNodesWithFunction(FunctionFactory::instance().get("and", context), not_equals_conditions, std::make_shared<ConstantNode>(Field(true)));
        }
        Tuple tuple;
        for (const auto & key_value_pair : key_value_pairs)
            tuple.emplace_back(key_value_pair.first.getValue());
        if (tuple.size() == 1)
            return notEquals(case_column, *std::make_shared<ConstantNode>(tuple.front()), context);
        auto not_in_function_resolver = FunctionFactory::instance().get("notIn", context);
        auto not_in_node = createFunctionNode(not_in_function_resolver, case_column, std::make_shared<ConstantNode>(tuple));
        auto is_null_node = createFunctionNode(FunctionFactory::instance().get("isNull", context), case_column);
        return createFunctionNode(FunctionFactory::instance().get("or", context), not_in_node, is_null_node);
    }

    QueryTreeNodePtr replaceImpl() override
    {
        QueryTreeNodes equals_conditions;
        for (const auto & item : right_value_map)
            equals_conditions.emplace_back(equals(case_column, *item.second, context));
        return combineNodesWithFunction(FunctionFactory::instance().get("or", context), equals_conditions, nullptr);
    }
};

class CaseWhenNotEqualsReplace : public ICaseWhenReplaceAlogrithm
{
public:
    CaseWhenNotEqualsReplace(const FunctionNode & case_node, const ConstantNode & right_node, const ContextPtr & context_)
        : ICaseWhenReplaceAlogrithm(case_node, right_node, context_)
    {
    }

protected:
    QueryTreeNodePtr replaceImplNoElse() override
    {
        QueryTreeNodes conditions;

        for (const auto & item : key_value_pairs)
        {
            if (std::find(right_values.begin(), right_values.end(), item.second.getValue()) == right_values.end())
                conditions.emplace_back(equals(case_column, item.first, context));
        }
        return combineNodesWithFunction(FunctionFactory::instance().get("or", context), conditions, std::make_shared<ConstantNode>(Field(false)));
    }

    QueryTreeNodePtr replaceImplNotFound() override
    {
        // <> NULL is always False
        if (right_values.empty())
            return std::make_shared<ConstantNode>(Field(false));
        if (!has_else)
        {
            QueryTreeNodes conditions;
            for (const auto & item : key_value_pairs)
            {
                conditions.emplace_back(equals(case_column, item.first, context));
            }
            return combineNodesWithFunction(FunctionFactory::instance().get("or", context), conditions, nullptr);
        }
        return std::make_shared<ConstantNode>(Field(true));
    }
    QueryTreeNodePtr replaceImplElse() override
    {
        Tuple tuple;
        for (const auto & key_value_pair : key_value_pairs)
            tuple.emplace_back(key_value_pair.first.getValue());
        if (tuple.size() == 1)
            return notEquals(case_column, *std::make_shared<ConstantNode>(tuple.front()), context);
        auto resolver = FunctionFactory::instance().get("in", context);
        return createFunctionNode(resolver, case_column, std::make_shared<ConstantNode>(tuple));
    }

    QueryTreeNodePtr replaceImpl() override
    {
        QueryTreeNodes conditions;
        bool key_has_null = false;
        for (const auto & item : right_value_map)
        {
            conditions.emplace_back(notEquals(case_column, *item.second, context));
            if (item.second->getValue().isNull())
                key_has_null = true;
        }
        auto not_equals_node = combineNodesWithFunction(FunctionFactory::instance().get("and", context), conditions, nullptr);
        if (key_has_null)
            return not_equals_node;
        auto is_null_node = createFunctionNode(FunctionFactory::instance().get("isNull", context), case_column);
        return createFunctionNode(FunctionFactory::instance().get("or", context), not_equals_node, is_null_node);
    }
};

class CaseWhenInReplace : public ICaseWhenReplaceAlogrithm
{
public:
    CaseWhenInReplace(const FunctionNode & case_node, const ConstantNode & right_node, const ContextPtr & context_)
        : ICaseWhenReplaceAlogrithm(case_node, right_node, context_)
    {
    }

    bool support() override
    {
        if (has_else)
        {
            for (const auto & right_value : right_values)
            {
                if (right_value == else_node->getValue())
                    return false;
            }
        }
        return ICaseWhenReplaceAlogrithm::support();
    }

protected:
    QueryTreeNodePtr replaceImplNotFound() override { return std::make_shared<ConstantNode>(Field(false)); }

    QueryTreeNodePtr replaceImplElse() override
    {
        Tuple tuple;
        for (const auto & key_value_pair : key_value_pairs)
            tuple.emplace_back(key_value_pair.first.getValue());
        if (keys_contain_null)
        {
            QueryTreeNodes conditions;
            for (const auto & field : tuple)
                conditions.emplace_back(notEquals(case_column, *std::make_shared<ConstantNode>(field), context));
            return combineNodesWithFunction(FunctionFactory::instance().get("and", context), conditions, nullptr);
        }
        if (tuple.size() == 1)
            return notEquals(case_column, *std::make_shared<ConstantNode>(tuple.front()), context);
        return createFunctionNode(
            FunctionFactory::instance().get("notIn", context), case_column, std::make_shared<ConstantNode>(tuple));
    }

    QueryTreeNodePtr replaceImpl() override
    {
        QueryTreeNodes conditions;
        for (const auto & item : right_value_map)
        {
            conditions.emplace_back(equals(case_column, *item.second, context));
        }
        return combineNodesWithFunction(FunctionFactory::instance().get("or", context), conditions, nullptr);
    }
};

class CaseWhenNotInReplace : public ICaseWhenReplaceAlogrithm
{
public:
    CaseWhenNotInReplace(const FunctionNode & case_node, const ConstantNode & right_node, const ContextPtr & context_)
        : ICaseWhenReplaceAlogrithm(case_node, right_node, context_)
    {
    }

    bool support() override
    {
        if (has_else)
        {
            for (const auto & right_value : right_values)
            {
                if (right_value == else_node->getValue())
                    return false;
            }
        }
        return ICaseWhenReplaceAlogrithm::support();
    }
protected:
    QueryTreeNodePtr replaceImplNotFound() override { return std::make_shared<ConstantNode>(Field(true)); }

    QueryTreeNodePtr replaceImplElse() override
    {
        Tuple tuple;
        for (const auto & key_value_pair : key_value_pairs)
            tuple.emplace_back(key_value_pair.first.getValue());
        if (keys_contain_null)
        {
            QueryTreeNodes conditions;
            for (const auto & field : tuple)
                conditions.emplace_back(equals(case_column, *std::make_shared<ConstantNode>(field), context));
            return combineNodesWithFunction(FunctionFactory::instance().get("or", context), conditions, nullptr);
        }
        if (tuple.size() == 1)
            return equals(case_column, *std::make_shared<ConstantNode>(tuple.front()), context);
        return createFunctionNode(
            FunctionFactory::instance().get("in", context), case_column, std::make_shared<ConstantNode>(tuple));
    }

    QueryTreeNodePtr replaceImpl() override
    {
        QueryTreeNodes conditions;
        for (const auto & item : right_value_map)
        {
            conditions.emplace_back(notEquals(case_column, *item.second, context));
        }
        auto result = combineNodesWithFunction(FunctionFactory::instance().get("and", context), conditions, nullptr);
        if (!keys_contain_null)
        {
            auto is_null_node = createFunctionNode(FunctionFactory::instance().get("isNull", context), case_column);
            return createFunctionNode(FunctionFactory::instance().get("or", context), result, is_null_node);
        }
        return result;

    }
};

class CaseWhenSimplifyPassVisitor : public InDepthQueryTreeVisitorWithContext<CaseWhenSimplifyPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CaseWhenSimplifyPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        // if parent has isNull or isNotNull function, abandon the optimization
        if (in_disabled_function)
            return;
        // null property is hard to handle, so abandon the optimization
        static const std::unordered_set<String> disabled_parent_funcs = {"isNull", "isNotNull"};
        static const std::unordered_set<String> supported_funcs = {"in", "notIn", "equals", "notEquals"};
        auto * func_node = node->as<FunctionNode>();
        if (!func_node)
            return;
        if (disabled_parent_funcs.contains(func_node->getFunctionName()))
        {
            in_disabled_function = true;
            return;
        }

        if (!checkFunctionWithArguments(node, supported_funcs, 2))
            return;
        if (!checkFunctionArgumentsType<FunctionNode, ConstantNode>(func_node))
            return;
        auto * case_node = func_node->getArguments().getNodes()[0]->as<FunctionNode>();
        auto * value_node = func_node->getArguments().getNodes()[1]->as<ConstantNode>();
        auto case_args_num = case_node->getArguments().getNodes().size();
        if (case_node->getFunctionName() != "caseWithExpression" || case_args_num < 4 || case_args_num % 2 != 0)
            return;
        QueryTreeNodePtr new_node;
        if (func_node->getFunctionName() == "equals")
        {
            CaseWhenEqualsReplace replace(*case_node, *value_node, getContext());
            new_node = replace.replace();

        }
        else if (func_node->getFunctionName() == "notEquals")
        {
            CaseWhenNotEqualsReplace replace(*case_node, *value_node, getContext());
            new_node = replace.replace();
        }
        else if (func_node->getFunctionName() == "in")
        {
            CaseWhenInReplace replace(*case_node, *value_node, getContext());
            new_node = replace.replace();
        }
        else if (func_node->getFunctionName() == "notIn")
        {
            CaseWhenNotInReplace replace(*case_node, *value_node, getContext());
            new_node = replace.replace();
        }
        if (new_node && new_node->getResultType()->equals(*node->getResultType()))
            node = new_node;
    }

private:
    bool in_disabled_function = false;
};
}

void CaseWhenSimplifyPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    CaseWhenSimplifyPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}
}
