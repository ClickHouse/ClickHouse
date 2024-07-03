#include <Analyzer/Passes/OptimizeDateOrDateTimeConverterWithPreimagePass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

class OptimizeDateOrDateTimeConverterWithPreimageVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeDateOrDateTimeConverterWithPreimageVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeDateOrDateTimeConverterWithPreimageVisitor>;

    explicit OptimizeDateOrDateTimeConverterWithPreimageVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

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
        {
            return !relations.contains(function->getFunctionName());
        }

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

        const auto * function = node->as<FunctionNode>();

        if (!function || !swap_relations.contains(function->getFunctionName())) return;

        if (function->getArguments().getNodes().size() != 2) return;

        size_t func_id = function->getArguments().getNodes().size();

        for (size_t i = 0; i < function->getArguments().getNodes().size(); i++)
        {
            if (const auto * func = function->getArguments().getNodes()[i]->as<FunctionNode>())
            {
                func_id = i;
            }
        }

        if (func_id == function->getArguments().getNodes().size()) return;

        size_t literal_id = 1 - func_id;
        const auto * literal = function->getArguments().getNodes()[literal_id]->as<ConstantNode>();

        if (!literal || literal->getValue().getType() != Field::Types::UInt64) return;

        String comparator = literal_id > func_id ? function->getFunctionName(): swap_relations.at(function->getFunctionName());

        const auto * func_node = function->getArguments().getNodes()[func_id]->as<FunctionNode>();
        /// Currently we only handle single-argument functions.
        if (!func_node || func_node->getArguments().getNodes().size() != 1) return;

        const auto * column_id = func_node->getArguments().getNodes()[0]->as<ColumnNode>();
        if (!column_id) return;

        const auto * column_type = column_id->getColumnType().get();
        if (!isDateOrDate32(column_type) && !isDateTime(column_type) && !isDateTime64(column_type)) return;

        const auto & converter = FunctionFactory::instance().tryGet(func_node->getFunctionName(), getContext());
        if (!converter) return;

        ColumnsWithTypeAndName args;
        args.emplace_back(column_id->getColumnType(), "tmp");
        auto converter_base = converter->build(args);
        if (!converter_base || !converter_base->hasInformationAboutPreimage()) return;

        auto preimage_range = converter_base->getPreimage(*(column_id->getColumnType()), literal->getValue());
        if (!preimage_range) return;

        const auto new_node = generateOptimizedDateFilter(comparator, *column_id, *preimage_range);

        if (!new_node) return;

        node = new_node;
    }

private:
    QueryTreeNodePtr generateOptimizedDateFilter(const String & comparator, const ColumnNode & column_node, const std::pair<Field, Field>& range) const
    {
        const DateLUTImpl & date_lut = DateLUT::instance("UTC");

        String start_date_or_date_time;
        String end_date_or_date_time;

        if (isDateOrDate32(column_node.getColumnType().get()))
        {
            start_date_or_date_time = date_lut.dateToString(range.first.get<DateLUTImpl::Time>());
            end_date_or_date_time = date_lut.dateToString(range.second.get<DateLUTImpl::Time>());
        }
        else if (isDateTime(column_node.getColumnType().get()) || isDateTime64(column_node.getColumnType().get()))
        {
            start_date_or_date_time = date_lut.timeToString(range.first.get<DateLUTImpl::Time>());
            end_date_or_date_time = date_lut.timeToString(range.second.get<DateLUTImpl::Time>());
        }
        else [[unlikely]] return {};

        if (comparator == "equals")
        {
            const auto lhs = std::make_shared<FunctionNode>("greaterOrEquals");
            lhs->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node.getColumn(), column_node.getColumnSource()));
            lhs->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(start_date_or_date_time));
            resolveOrdinaryFunctionNode(*lhs, lhs->getFunctionName());

            const auto rhs = std::make_shared<FunctionNode>("less");
            rhs->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node.getColumn(), column_node.getColumnSource()));
            rhs->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(end_date_or_date_time));
            resolveOrdinaryFunctionNode(*rhs, rhs->getFunctionName());

            const auto new_date_filter = std::make_shared<FunctionNode>("and");
            new_date_filter->getArguments().getNodes() = {lhs, rhs};
            resolveOrdinaryFunctionNode(*new_date_filter, new_date_filter->getFunctionName());

            return new_date_filter;
        }
        else if (comparator == "notEquals")
        {
            const auto lhs = std::make_shared<FunctionNode>("less");
            lhs->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node.getColumn(), column_node.getColumnSource()));
            lhs->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(start_date_or_date_time));
            resolveOrdinaryFunctionNode(*lhs, lhs->getFunctionName());

            const auto rhs = std::make_shared<FunctionNode>("greaterOrEquals");
            rhs->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node.getColumn(), column_node.getColumnSource()));
            rhs->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(end_date_or_date_time));
            resolveOrdinaryFunctionNode(*rhs, rhs->getFunctionName());

            const auto new_date_filter = std::make_shared<FunctionNode>("or");
            new_date_filter->getArguments().getNodes() = {lhs, rhs};
            resolveOrdinaryFunctionNode(*new_date_filter, new_date_filter->getFunctionName());

            return new_date_filter;
        }
        else if (comparator == "greater")
        {
            const auto new_date_filter = std::make_shared<FunctionNode>("greaterOrEquals");
            new_date_filter->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node.getColumn(), column_node.getColumnSource()));
            new_date_filter->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(end_date_or_date_time));
            resolveOrdinaryFunctionNode(*new_date_filter, new_date_filter->getFunctionName());

            return new_date_filter;
        }
        else if (comparator == "lessOrEquals")
        {
            const auto new_date_filter = std::make_shared<FunctionNode>("less");
            new_date_filter->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node.getColumn(), column_node.getColumnSource()));
            new_date_filter->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(end_date_or_date_time));
            resolveOrdinaryFunctionNode(*new_date_filter, new_date_filter->getFunctionName());

            return new_date_filter;
        }
        else if (comparator == "less" || comparator == "greaterOrEquals")
        {
            const auto new_date_filter = std::make_shared<FunctionNode>(comparator);
            new_date_filter->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column_node.getColumn(), column_node.getColumnSource()));
            new_date_filter->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(start_date_or_date_time));
            resolveOrdinaryFunctionNode(*new_date_filter, new_date_filter->getFunctionName());

            return new_date_filter;
        }
        else [[unlikely]]
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected equals, notEquals, less, lessOrEquals, greater, greaterOrEquals. Actual {}",
                comparator);
        }
    }

    void resolveOrdinaryFunctionNode(FunctionNode & function_node, const String & function_name) const
    {
        auto function = FunctionFactory::instance().get(function_name, getContext());
        function_node.resolveAsFunction(function->build(function_node.getArgumentColumns()));
    }
};

}

void OptimizeDateOrDateTimeConverterWithPreimagePass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    OptimizeDateOrDateTimeConverterWithPreimageVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
