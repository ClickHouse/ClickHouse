#include <Planner/CorrelatedSubqueries.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>

#include <Functions/IFunction.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <Planner/Planner.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}


namespace
{

class ExecuteCorrelatedSubqueryFunction : public IExecutableFunction
{
public:
    explicit ExecuteCorrelatedSubqueryFunction(String name_, QueryTreeNodePtr subquery_)
        : name(std::move(name_))
        , subquery(std::move(subquery_))
    {
        auto * query_node = subquery->as<QueryNode>();
        auto * union_node = subquery->as<UnionNode>();
        if (!query_node && !union_node)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected QUERY or UNION node. Actual {}", subquery->formatASTForErrorMessage());

        subquery_arguments = query_node ? query_node->getArguments().getNodes() : union_node->getArguments().getNodes();

        auto subquery_context = query_node ? query_node->getMutableContext() : union_node->getMutableContext();
        context = Context::createCopy(subquery_context);

        Settings subquery_settings = context->getSettings();
        subquery_settings.max_result_rows = 1;
        subquery_settings.extremes = false;
        context->setSettings(subquery_settings);

        select_query_options.is_subquery = true;
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto result_column = result_type->createColumn();

        if (input_rows_count == 0)
            return result_column->cloneResized(input_rows_count);

        size_t arguments_size = arguments.size();
        Field column_constant_value;

        IQueryTreeNode::ReplacementMap replacement_map;

        for (size_t row_index = 0; row_index < input_rows_count; ++row_index)
        {
            for (size_t argument_index = 0; argument_index < arguments_size; ++argument_index)
            {
                const auto & argument = arguments[argument_index];
                argument.column->get(row_index, column_constant_value);

                const auto & subquery_argument = subquery_arguments.at(argument_index);
                auto constant_node = std::make_shared<ConstantNode>(column_constant_value, subquery_argument->getResultType());
                replacement_map[subquery_argument.get()] = constant_node;
            }

            auto subquery_to_execute = subquery->cloneAndReplace(replacement_map);

            auto subquery_result_column = evaluateScalarSubquery(subquery_to_execute, result_type);
            chassert(subquery_result_column->size() == 1);

            result_column->insertFrom(*subquery_result_column, 0);
        }

        return result_column;
    }

private:
    ColumnPtr evaluateScalarSubquery(const QueryTreeNodePtr & node, const DataTypePtr & result_type) const
    {
        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(node->toAST(), context, select_query_options);

        auto io = interpreter->execute();
        PullingAsyncPipelineExecutor executor(io.pipeline);
        io.pipeline.setProgressCallback(context->getProgressCallback());
        io.pipeline.setProcessListElement(context->getProcessListElement());

        Block block;

        while (block.rows() == 0 && executor.pull(block))
        {
        }

        if (block.rows() == 0)
        {
            if (!result_type->canBeInsideNullable())
                throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                    "Scalar subquery returned empty result of type {} which cannot be Nullable",
                    result_type->getName());

            auto result_column = result_type->createColumn();
            result_column->insert(Null());
            return result_column;
        }

        if (block.rows() != 1)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

        Block tmp_block;
        while (tmp_block.rows() == 0 && executor.pull(tmp_block))
        {
        }

        if (tmp_block.rows() != 0)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

        block = materializeBlock(block);
        size_t columns = block.columns();
        if (columns == 1)
            return castColumn(block.getColumnsWithTypeAndName()[0], result_type);

        return ColumnTuple::create(block.getColumns());
    }

    String name;
    QueryTreeNodePtr subquery;
    QueryTreeNodes subquery_arguments;
    ContextMutablePtr context;
    SelectQueryOptions select_query_options;
};

class ExecuteCorrelatedSubqueryFunctionBase : public IFunctionBase
{
public:
    static constexpr auto name = "executeCorrelatedSubquery";

    explicit ExecuteCorrelatedSubqueryFunctionBase(QueryTreeNodePtr subquery_, DataTypes argument_types_)
        : subquery(std::move(subquery_))
        , argument_types(std::move(argument_types_))
        , result_type(subquery->getResultType())
    {
    }

    bool isCompilable() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return true;
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return result_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecuteCorrelatedSubqueryFunction>(name, subquery);
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
private:
    QueryTreeNodePtr subquery;
    DataTypes argument_types;
    DataTypePtr result_type;
};

}

QueryTreeNodePtr buildFunctionNodeToExecuteCorrelatedSubquery(const QueryTreeNodePtr & node)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();
    if (!query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected QUERY or UNION node. Actual {}", node->formatASTForErrorMessage());

    auto & arguments_nodes = query_node ? query_node->getArguments().getNodes() : union_node->getArguments().getNodes();
    if (arguments_nodes.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected correlated QUERY or UNION node subquery");

    DataTypes argument_types;
    argument_types.reserve(arguments_nodes.size());

    for (auto & argument_node : arguments_nodes)
    {
        auto & argument_column_node = argument_node->as<ColumnNode &>();
        argument_types.push_back(argument_column_node.getColumnType());
    }

    auto execute_correlated_subquery_function_base = std::make_shared<ExecuteCorrelatedSubqueryFunctionBase>(node, std::move(argument_types));
    auto execute_correlated_subquery_function_node = std::make_shared<FunctionNode>(execute_correlated_subquery_function_base->getName());
    execute_correlated_subquery_function_node->resolveAsFunction(execute_correlated_subquery_function_base);
    execute_correlated_subquery_function_node->getArguments().getNodes() = arguments_nodes;

    return execute_correlated_subquery_function_node;
}

}
