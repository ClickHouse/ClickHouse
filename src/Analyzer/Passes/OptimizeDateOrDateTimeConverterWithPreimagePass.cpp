#include <Analyzer/Passes/OptimizeDateOrDateTimeConverterWithPreimagePass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_time_filter_with_preimage;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

class OptimizeDateOrDateTimeConverterWithPreimageVisitor
    : public InDepthQueryTreeVisitorWithContext<OptimizeDateOrDateTimeConverterWithPreimageVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeDateOrDateTimeConverterWithPreimageVisitor>;

    explicit OptimizeDateOrDateTimeConverterWithPreimageVisitor(ContextPtr context) : Base(std::move(context)) { }

    static bool needChildVisit(QueryTreeNodePtr & node, QueryTreeNodePtr & /*child*/)
    {
        const static std::unordered_set<String> relations = {
            "equals",
            "notEquals",
            "less",
            "greater",
            "lessOrEquals",
            "greaterOrEquals",
        };

        if (const auto * function = node->as<FunctionNode>())
            return !relations.contains(function->getFunctionName());

        return true;
    }

    void enterImpl(QueryTreeNodePtr & node) const
    {
        const static std::unordered_map<String, String> swap_relations = {
            {"equals", "equals"},
            {"notEquals", "notEquals"},
            {"less", "greater"},
            {"greater", "less"},
            {"lessOrEquals", "greaterOrEquals"},
            {"greaterOrEquals", "lessOrEquals"},
        };

        if (!getSettings()[Setting::optimize_time_filter_with_preimage])
            return;

        const auto * function = node->as<FunctionNode>();

        if (!function || !swap_relations.contains(function->getFunctionName()))
            return;

        if (function->getArguments().getNodes().size() != 2)
            return;

        size_t func_id = function->getArguments().getNodes().size();

        for (size_t i = 0; i < function->getArguments().getNodes().size(); i++)
        {
            if (const auto * /*func*/ _ = function->getArguments().getNodes()[i]->as<FunctionNode>())
            {
                func_id = i;
                break;
            }
        }

        if (func_id == function->getArguments().getNodes().size())
            return;

        size_t literal_id = 1 - func_id;
        const auto * literal = function->getArguments().getNodes()[literal_id]->as<ConstantNode>();

        if (!literal || !literal->getResultType()->isValueRepresentedByUnsignedInteger())
            return;

        String comparator = literal_id > func_id ? function->getFunctionName() : swap_relations.at(function->getFunctionName());

        const auto * func_node = function->getArguments().getNodes()[func_id]->as<FunctionNode>();
        /// Currently we only handle single-argument functions.
        if (!func_node || func_node->getArguments().getNodes().size() != 1)
            return;

        const auto & argument_node = func_node->getArguments().getNodes()[0];
        const auto * column_id = argument_node->as<ColumnNode>();
        if (!column_id)
            return;

        if (column_id->getColumnName() == "__grouping_set")
            return;

        const auto * column_type = column_id->getColumnType().get();
        if (!isDateOrDate32(column_type) && !isDateTime(column_type) && !isDateTime64(column_type))
            return;

        const auto & converter = FunctionFactory::instance().tryGet(func_node->getFunctionName(), getContext());
        if (!converter)
            return;

        ColumnsWithTypeAndName args;
        args.emplace_back(column_id->getColumnType(), "tmp");
        auto converter_base = converter->build(args);
        if (!converter_base || !converter_base->hasInformationAboutPreimage())
            return;

        auto preimage_range = converter_base->getPreimage(*(column_id->getColumnType()), literal->getValue());
        if (!preimage_range)
            return;

        const auto new_node = generateOptimizedDateFilter(comparator, argument_node, *preimage_range);

        if (!new_node)
            return;

        node = new_node;
    }

private:
    QueryTreeNodePtr generateOptimizedDateFilter(
        const String & comparator, const QueryTreeNodePtr & column_node, const std::pair<Field, Field> & range) const
    {
        const DateLUTImpl & date_lut = DateLUT::instance("UTC");

        String start_date_or_date_time;
        String end_date_or_date_time;

        const auto & column_node_typed = column_node->as<ColumnNode &>();
        const auto & column_type = column_node_typed.getColumnType().get();
        if (isDateOrDate32(column_type))
        {
            start_date_or_date_time = date_lut.dateToString(range.first.safeGet<DateLUTImpl::Time>());
            end_date_or_date_time = date_lut.dateToString(range.second.safeGet<DateLUTImpl::Time>());
        }
        else if (isDateTime(column_type) || isDateTime64(column_type))
        {
            start_date_or_date_time = date_lut.timeToString(range.first.safeGet<DateLUTImpl::Time>());
            end_date_or_date_time = date_lut.timeToString(range.second.safeGet<DateLUTImpl::Time>());
        }
        else [[unlikely]]
            return {};

        if (comparator == "equals")
        {
            return createFunctionNode(
                "and",
                createFunctionNode("greaterOrEquals", column_node, std::make_shared<ConstantNode>(start_date_or_date_time)),
                createFunctionNode("less", column_node, std::make_shared<ConstantNode>(end_date_or_date_time)));
        }
        if (comparator == "notEquals")
        {
            return createFunctionNode(
                "or",
                createFunctionNode("less", column_node, std::make_shared<ConstantNode>(start_date_or_date_time)),
                createFunctionNode("greaterOrEquals", column_node, std::make_shared<ConstantNode>(end_date_or_date_time)));
        }
        if (comparator == "greater")
        {
            return createFunctionNode("greaterOrEquals", column_node, std::make_shared<ConstantNode>(end_date_or_date_time));
        }
        if (comparator == "lessOrEquals")
        {
            return createFunctionNode("less", column_node, std::make_shared<ConstantNode>(end_date_or_date_time));
        }
        if (comparator == "less" || comparator == "greaterOrEquals")
        {
            return createFunctionNode(comparator, column_node, std::make_shared<ConstantNode>(start_date_or_date_time));
        }
        [[unlikely]] {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected equals, notEquals, less, lessOrEquals, greater, greaterOrEquals. Actual {}",
                comparator);
        }
    }

    template <typename... Args>
    QueryTreeNodePtr createFunctionNode(const String & function_name, Args &&... args) const
    {
        auto function = FunctionFactory::instance().get(function_name, getContext());
        const auto function_node = std::make_shared<FunctionNode>(function_name);
        auto & new_arguments = function_node->getArguments().getNodes();
        new_arguments.reserve(sizeof...(args));
        (new_arguments.push_back(std::forward<Args>(args)), ...);
        function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));

        return function_node;
    }
};

}

void OptimizeDateOrDateTimeConverterWithPreimagePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeDateOrDateTimeConverterWithPreimageVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
