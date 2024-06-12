#include <algorithm>
#include <memory>
#include <Core/NamesAndTypes.h>

#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/misc.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/VirtualColumnUtils.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Columns/ColumnSet.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ActionsVisitor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Verifying that the function depends only on the specified columns
bool isValidFunction(const ASTPtr & expression, const std::function<bool(const ASTPtr &)> & is_constant)
{
    const auto * function = expression->as<ASTFunction>();
    if (function && functionIsInOrGlobalInOperator(function->name))
    {
        // Second argument of IN can be a scalar subquery
        return isValidFunction(function->arguments->children[0], is_constant);
    }
    else
        return is_constant(expression);
}

/// Extract all subfunctions of the main conjunction, but depending only on the specified columns
bool extractFunctions(const ASTPtr & expression, const std::function<bool(const ASTPtr &)> & is_constant, ASTs & result)
{
    const auto * function = expression->as<ASTFunction>();

    if (function)
    {
        if (function->name == "and" || function->name == "indexHint")
        {
            bool ret = true;
            for (const auto & child : function->arguments->children)
                ret &= extractFunctions(child, is_constant, result);
            return ret;
        }
        else if (function->name == "or")
        {
            bool ret = false;
            ASTs or_args;
            for (const auto & child : function->arguments->children)
                ret |= extractFunctions(child, is_constant, or_args);

            if (!or_args.empty())
            {
                /// In case of there are less number of arguments for which
                /// is_constant() == true, we need to add always-true
                /// implicitly to avoid breaking AND invariant.
                ///
                /// Consider the following:
                ///
                ///     ((value = 10) OR (_table = 'v2')) AND ((_table = 'v1') OR (value = 20))
                ///
                /// Without implicit always-true:
                ///
                ///     (_table = 'v2') AND (_table = 'v1')
                ///
                /// With:
                ///
                ///     (_table = 'v2' OR 1) AND (_table = 'v1' OR 1) -> (_table = 'v2') OR (_table = 'v1')
                ///
                if (or_args.size() != function->arguments->children.size())
                    or_args.push_back(std::make_shared<ASTLiteral>(Field(1)));
                result.push_back(makeASTForLogicalOr(std::move(or_args)));
            }
            return ret;
        }
    }

    if (isValidFunction(expression, is_constant))
    {
        result.push_back(expression->clone());
        return true;
    }
    else
        return false;
}

/// Construct a conjunction from given functions
ASTPtr buildWhereExpression(ASTs && functions)
{
    if (functions.empty())
        return nullptr;
    if (functions.size() == 1)
        return functions[0];
    return makeASTForLogicalAnd(std::move(functions));
}

}

namespace VirtualColumnUtils
{

void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value, const String & func)
{
    auto & select = ast->as<ASTSelectQuery &>();
    if (!select.with())
        select.setExpression(ASTSelectQuery::Expression::WITH, std::make_shared<ASTExpressionList>());

    if (func.empty())
    {
        auto literal = std::make_shared<ASTLiteral>(value);
        literal->alias = column_name;
        literal->prefer_alias_to_column_name = true;
        select.with()->children.push_back(literal);
    }
    else
    {
        auto literal = std::make_shared<ASTLiteral>(value);
        literal->prefer_alias_to_column_name = true;

        auto function = makeASTFunction(func, literal);
        function->alias = column_name;
        function->prefer_alias_to_column_name = true;
        select.with()->children.push_back(function);
    }
}

bool prepareFilterBlockWithQuery(const ASTPtr & query, ContextPtr context, Block block, ASTPtr & expression_ast)
{
    if (block.rows() == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot prepare filter with empty block");

    /// Take the first row of the input block to build a constant block
    auto columns = block.getColumns();
    Columns const_columns(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (isColumnConst(*columns[i]))
            const_columns[i] = columns[i]->cloneResized(1);
        else
            const_columns[i] = ColumnConst::create(columns[i]->cloneResized(1), 1);
    }

    block.setColumns(const_columns);

    bool unmodified = true;
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where() && !select.prewhere())
        return unmodified;

    // Provide input columns as constant columns to check if an expression is
    // constant and depends on the columns from provided block (the last is
    // required to allow skipping some conditions for handling OR).
    std::function<bool(const ASTPtr &)> is_constant = [&block, &context](const ASTPtr & expr)
    {
        auto actions = std::make_shared<ActionsDAG>(block.getColumnsWithTypeAndName());
        PreparedSetsPtr prepared_sets = std::make_shared<PreparedSets>();
        const NamesAndTypesList source_columns;
        const NamesAndTypesList aggregation_keys;
        const ColumnNumbersList grouping_set_keys;

        ActionsVisitor::Data visitor_data(
            context, SizeLimits{}, 1, source_columns, std::move(actions), prepared_sets, true, true, true,
            { aggregation_keys, grouping_set_keys, GroupByKind::NONE });

        ActionsVisitor(visitor_data).visit(expr);
        actions = visitor_data.getActions();
        auto expr_column_name = expr->getColumnName();

        const auto * expr_const_node = actions->tryFindInOutputs(expr_column_name);
        if (!expr_const_node)
            return false;
        auto filter_actions = ActionsDAG::buildFilterActionsDAG({expr_const_node}, {}, context);
        const auto & nodes = filter_actions->getNodes();
        bool has_dependent_columns = std::any_of(nodes.begin(), nodes.end(), [&](const auto & node)
        {
            return block.has(node.result_name);
        });
        if (!has_dependent_columns)
            return false;

        auto expression_actions = std::make_shared<ExpressionActions>(actions);
        auto block_with_constants = block;
        expression_actions->execute(block_with_constants);
        return block_with_constants.has(expr_column_name) && isColumnConst(*block_with_constants.getByName(expr_column_name).column);
    };

    /// Create an expression that evaluates the expressions in WHERE and PREWHERE, depending only on the existing columns.
    ASTs functions;
    if (select.where())
        unmodified &= extractFunctions(select.where(), is_constant, functions);
    if (select.prewhere())
        unmodified &= extractFunctions(select.prewhere(), is_constant, functions);

    expression_ast = buildWhereExpression(std::move(functions));
    return unmodified;
}

void filterBlockWithQuery(const ASTPtr & query, Block & block, ContextPtr context, ASTPtr expression_ast)
{
    if (block.rows() == 0)
        return;

    if (!expression_ast)
        prepareFilterBlockWithQuery(query, context, block, expression_ast);

    if (!expression_ast)
        return;

    /// Let's analyze and calculate the prepared expression.
    auto syntax_result = TreeRewriter(context).analyze(expression_ast, block.getNamesAndTypesList());
    ExpressionAnalyzer analyzer(expression_ast, syntax_result, context);
    ExpressionActionsPtr actions = analyzer.getActions(false /* add alises */, true /* project result */, CompileExpressions::yes);

    for (const auto & node : actions->getNodes())
    {
        if (node.type == ActionsDAG::ActionType::COLUMN)
        {
            const ColumnSet * column_set = checkAndGetColumnConstData<const ColumnSet>(node.column.get());
            if (!column_set)
                column_set = checkAndGetColumn<const ColumnSet>(node.column.get());

            if (column_set)
            {
                auto future_set = column_set->getData();
                if (!future_set->get())
                {
                    if (auto * set_from_subquery = typeid_cast<FutureSetFromSubquery *>(future_set.get()))
                    {
                        auto plan = set_from_subquery->build(context);
                        auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
                        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
                        pipeline.complete(std::make_shared<EmptySink>(Block()));

                        CompletedPipelineExecutor executor(pipeline);
                        executor.execute();
                    }
                }
            }
        }
    }

    Block block_with_filter = block;
    actions->execute(block_with_filter);

    /// Filter the block.
    String filter_column_name = expression_ast->getColumnName();
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column->convertToFullColumnIfConst();

    ConstantFilterDescription constant_filter(*filter_column);

    if (constant_filter.always_true)
    {
        return;
    }

    if (constant_filter.always_false)
    {
        block = block.cloneEmpty();
        return;
    }

    FilterDescription filter(*filter_column);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnPtr & column = block.safeGetByPosition(i).column;
        column = column->filter(*filter.data, -1);
    }
}

NamesAndTypesList getPathAndFileVirtualsForStorage(NamesAndTypesList storage_columns)
{
    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    default_virtuals.sort();
    storage_columns.sort();

    NamesAndTypesList result_virtuals;
    std::set_difference(
        default_virtuals.begin(), default_virtuals.end(), storage_columns.begin(), storage_columns.end(),
        std::back_inserter(result_virtuals),
        [](const NameAndTypePair & lhs, const NameAndTypePair & rhs){ return lhs.name < rhs.name; });

    return result_virtuals;
}

static void addPathAndFileToVirtualColumns(Block & block, const String & path, size_t idx)
{
    if (block.has("_path"))
        block.getByName("_path").column->assumeMutableRef().insert(path);

    if (block.has("_file"))
    {
        auto pos = path.find_last_of('/');
        String file;
        if (pos != std::string::npos)
            file = path.substr(pos + 1);
        else
            file = path;

        block.getByName("_file").column->assumeMutableRef().insert(file);
    }

    block.getByName("_idx").column->assumeMutableRef().insert(idx);
}

ASTPtr createPathAndFileFilterAst(const ASTPtr & query, const NamesAndTypesList & virtual_columns, const String & path_example, const ContextPtr & context)
{
    if (!query || virtual_columns.empty())
        return {};

    Block block;
    for (const auto & column : virtual_columns)
        block.insert({column.type->createColumn(), column.type, column.name});
    /// Create a block with one row to construct filter
    /// Append "idx" column as the filter result
    block.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});
    addPathAndFileToVirtualColumns(block, path_example, 0);
    ASTPtr filter_ast;
    prepareFilterBlockWithQuery(query, context, block, filter_ast);
    return filter_ast;
}

ColumnPtr getFilterByPathAndFileIndexes(const std::vector<String> & paths, const ASTPtr & query, const NamesAndTypesList & virtual_columns, const ContextPtr & context, ASTPtr filter_ast)
{
    Block block;
    for (const auto & column : virtual_columns)
        block.insert({column.type->createColumn(), column.type, column.name});
    block.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

    for (size_t i = 0; i != paths.size(); ++i)
        addPathAndFileToVirtualColumns(block, paths[i], i);

    filterBlockWithQuery(query, block, context, filter_ast);

    return block.getByName("_idx").column;
}

void addRequestedPathAndFileVirtualsToChunk(
    Chunk & chunk, const NamesAndTypesList & requested_virtual_columns, const String & path, const String * filename)
{
    for (const auto & virtual_column : requested_virtual_columns)
    {
        if (virtual_column.name == "_path")
        {
            chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), path)->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_file")
        {
            if (filename)
            {
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), *filename)->convertToFullColumnIfConst());
            }
            else
            {
                size_t last_slash_pos = path.find_last_of('/');
                auto filename_from_path = path.substr(last_slash_pos + 1);
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), filename_from_path)->convertToFullColumnIfConst());
            }
        }
    }
}

}

}
