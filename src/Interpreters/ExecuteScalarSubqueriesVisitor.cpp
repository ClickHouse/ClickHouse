#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/queryToString.h>
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
    if (data.getContext()->hasQueryContext() && data.getContext()->getQueryContext()->hasScalar(scalar_query_hash_str))
        scalar = data.getContext()->getQueryContext()->getScalar(scalar_query_hash_str);
    else if (data.scalars.count(scalar_query_hash_str))
        scalar = data.scalars[scalar_query_hash_str];
    else
    {
        auto subquery_context = Context::createCopy(data.getContext());
        Settings subquery_settings = data.getContext()->getSettings();
        subquery_settings.max_result_rows = 1;
        subquery_settings.extremes = false;
        subquery_context->setSettings(subquery_settings);

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
                auto types = interpreter.getSampleBlock().getDataTypes();
                if (types.size() != 1)
                    types = {std::make_shared<DataTypeTuple>(types)};

                auto & type = types[0];
                if (!type->isNullable())
                {
                    if (!type->canBeInsideNullable())
                        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                                        "Scalar subquery returned empty result of type {} which cannot be Nullable",
                                        type->getName());

                    type = makeNullable(type);
                }

                ASTPtr ast_new = std::make_shared<ASTLiteral>(Null());
                ast_new = addTypeConversionToAST(std::move(ast_new), type->getName());

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
        {
            auto & column = block.getByPosition(0);
            /// Here we wrap type to nullable if we can.
            /// It is needed cause if subquery return no rows, it's result will be Null.
            /// In case of many columns, do not check it cause tuple can't be nullable.
            if (!column.type->isNullable() && column.type->canBeInsideNullable())
            {
                column.type = makeNullable(column.type);
                column.column = makeNullable(column.column);
            }
            scalar = block;
        }
        else
        {
            ColumnWithTypeAndName ctn;
            ctn.type = std::make_shared<DataTypeTuple>(block.getDataTypes());
            ctn.column = ColumnTuple::create(block.getColumns());
            scalar.insert(ctn);
        }
    }

    const Settings & settings = data.getContext()->getSettingsRef();

    // Always convert to literals when there is no query context.
    if (data.only_analyze || !settings.enable_scalar_subquery_optimization || worthConvertingToLiteral(scalar)
        || !data.getContext()->hasQueryContext())
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
    /// But if an argument is not subquery, then deeper may be scalar subqueries and we need to descend in them.

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
