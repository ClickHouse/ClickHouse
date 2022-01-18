#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithElement.h>

#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnTuple.h>

#include <IO/WriteHelpers.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}


bool ExecuteScalarSubqueriesMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    /// Processed
    if (node->as<ASTSubquery>() || node->as<ASTFunction>())
        return false;

    /// Don't descend into subqueries in FROM section
    if (node->as<ASTTableExpression>())
        return false;

    /// Do not go to subqueries defined in with statement
    if (node->as<ASTWithElement>())
        return false;

    if (node->as<ASTSelectQuery>())
    {
        /// Do not go to FROM, JOIN, UNION.
        if (child->as<ASTTableExpression>() || child->as<ASTSelectQuery>())
            return false;
    }

    return true;
}

void ExecuteScalarSubqueriesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (const auto * t = ast->as<ASTSubquery>())
        visit(*t, ast, data);
    if (const auto * t = ast->as<ASTFunction>())
        visit(*t, ast, data);
}

/// Converting to literal values might take a fair amount of overhead when the value is large, (e.g.
///  Array, BitMap, etc.), This conversion is required for constant folding, index lookup, branch
///  elimination. However, these optimizations should never be related to large values, thus we
///  blacklist them here.
static bool worthConvertingToLiteral(const Block & scalar)
{
    const auto * scalar_type_name = scalar.safeGetByPosition(0).type->getFamilyName();
    std::set<String> useless_literal_types = {"Array", "Tuple", "AggregateFunction", "Function", "Set", "LowCardinality"};
    return !useless_literal_types.count(scalar_type_name);
}

void ExecuteScalarSubqueriesMatcher::visit(const ASTSubquery & subquery, ASTPtr & ast, Data & data)
{
    auto hash = subquery.getTreeHash();
    auto scalar_query_hash_str = toString(hash.first) + "_" + toString(hash.second);

    Block scalar;
    if (data.context.hasQueryContext() && data.context.getQueryContext().hasScalar(scalar_query_hash_str))
        scalar = data.context.getQueryContext().getScalar(scalar_query_hash_str);
    else if (data.scalars.count(scalar_query_hash_str))
        scalar = data.scalars[scalar_query_hash_str];
    else
    {
        Context subquery_context = data.context;
        Settings subquery_settings = data.context.getSettings();
        subquery_settings.max_result_rows = 1;
        subquery_settings.extremes = false;
        subquery_context.setSettings(subquery_settings);

        ASTPtr subquery_select = subquery.children.at(0);

        auto options = SelectQueryOptions(QueryProcessingStage::Complete, data.subquery_depth + 1, true);
        options.analyze(data.only_analyze);

        auto interpreter = InterpreterSelectWithUnionQuery(subquery_select, subquery_context, options);
        Block block;

        if (data.only_analyze)
        {
            /// If query is only analyzed, then constants are not correct.
            block = interpreter.getSampleBlock();
            for (auto & column : block)
            {
                if (column.column->empty())
                {
                    auto mut_col = column.column->cloneEmpty();
                    mut_col->insertDefault();
                    column.column = std::move(mut_col);
                }
            }
        }
        else
        {
            auto io = interpreter.execute();

            PullingAsyncPipelineExecutor executor(io.pipeline);
            while (block.rows() == 0 && executor.pull(block));

            if (block.rows() == 0)
            {
                /// Interpret subquery with empty result as Null literal
                auto ast_new = std::make_unique<ASTLiteral>(Null());
                ast_new->setAlias(ast->tryGetAlias());
                ast = std::move(ast_new);
                return;
            }

            if (block.rows() != 1)
                throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);

            Block tmp_block;
            while (tmp_block.rows() == 0 && executor.pull(tmp_block));

            if (tmp_block.rows() != 0)
                throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
        }

        block = materializeBlock(block);
        size_t columns = block.columns();

        if (columns == 1)
            scalar = block;
        else
        {

            ColumnWithTypeAndName ctn;
            ctn.type = std::make_shared<DataTypeTuple>(block.getDataTypes());
            ctn.column = ColumnTuple::create(block.getColumns());
            scalar.insert(ctn);
        }
    }

    const Settings & settings = data.context.getSettingsRef();

    // Always convert to literals when there is no query context.
    if (data.only_analyze || !settings.enable_scalar_subquery_optimization || worthConvertingToLiteral(scalar) || !data.context.hasQueryContext())
    {
        /// subquery and ast can be the same object and ast will be moved.
        /// Save these fields to avoid use after move.
        auto alias = subquery.alias;
        auto prefer_alias_to_column_name = subquery.prefer_alias_to_column_name;

        auto lit = std::make_unique<ASTLiteral>((*scalar.safeGetByPosition(0).column)[0]);
        lit->alias = alias;
        lit->prefer_alias_to_column_name = prefer_alias_to_column_name;
        ast = addTypeConversionToAST(std::move(lit), scalar.safeGetByPosition(0).type->getName());

        /// If only analyze was requested the expression is not suitable for constant folding, disable it.
        if (data.only_analyze)
        {
            ast->as<ASTFunction>()->alias.clear();
            auto func = makeASTFunction("identity", std::move(ast));
            func->alias = alias;
            func->prefer_alias_to_column_name = prefer_alias_to_column_name;
            ast = std::move(func);
        }
    }
    else
    {
        auto func = makeASTFunction("__getScalar", std::make_shared<ASTLiteral>(scalar_query_hash_str));
        func->alias = subquery.alias;
        func->prefer_alias_to_column_name = subquery.prefer_alias_to_column_name;
        ast = std::move(func);
    }

    data.scalars[scalar_query_hash_str] = std::move(scalar);
}

void ExecuteScalarSubqueriesMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data & data)
{
    /// Don't descend into subqueries in arguments of IN operator.
    /// But if an argument is not subquery, than deeper may be scalar subqueries and we need to descend in them.

    std::vector<ASTPtr *> out;
    if (checkFunctionIsInOrGlobalInOperator(func))
    {
        for (auto & child : ast->children)
        {
            if (child != func.arguments)
                out.push_back(&child);
            else
                for (size_t i = 0, size = func.arguments->children.size(); i < size; ++i)
                    if (i != 1 || !func.arguments->children[i]->as<ASTSubquery>())
                        out.push_back(&func.arguments->children[i]);
        }
    }
    else
        for (auto & child : ast->children)
            out.push_back(&child);

    for (ASTPtr * add_node : out)
        Visitor(data).visit(*add_node);
}

}
