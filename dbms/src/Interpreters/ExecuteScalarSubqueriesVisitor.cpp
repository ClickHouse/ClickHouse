#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>

#include <Interpreters/Context.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int TOO_MANY_ROWS;
}


static ASTPtr addTypeConversion(std::unique_ptr<ASTLiteral> && ast, const String & type_name)
{
    auto func = std::make_shared<ASTFunction>();
    ASTPtr res = func;
    func->alias = ast->alias;
    func->prefer_alias_to_column_name = ast->prefer_alias_to_column_name;
    ast->alias.clear();
    func->name = "CAST";
    auto exp_list = std::make_shared<ASTExpressionList>();
    func->arguments = exp_list;
    func->children.push_back(func->arguments);
    exp_list->children.emplace_back(ast.release());
    exp_list->children.emplace_back(std::make_shared<ASTLiteral>(type_name));
    return res;
}

void ExecuteScalarSubqueriesMatcher::visit(const ASTSubquery & subquery, ASTPtr & ast, Data & data)
{
    Context subquery_context = data.context;
    Settings subquery_settings = data.context.getSettings();
    subquery_settings.max_result_rows = 1;
    subquery_settings.extremes = 0;
    subquery_context.setSettings(subquery_settings);

    ASTPtr subquery_select = subquery.children.at(0);
    BlockIO res = InterpreterSelectWithUnionQuery(
        subquery_select, subquery_context, {}, QueryProcessingStage::Complete, data.subquery_depth + 1).execute();

    Block block;
    try
    {
        block = res.in->read();

        if (!block)
        {
            /// Interpret subquery with empty result as Null literal
            auto ast_new = std::make_unique<ASTLiteral>(Null());
            ast_new->setAlias(ast->tryGetAlias());
            ast = std::move(ast_new);
            return;
        }

        if (block.rows() != 1 || res.in->read())
            throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::TOO_MANY_ROWS)
            throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
        else
            throw;
    }

    size_t columns = block.columns();
    if (columns == 1)
    {
        auto lit = std::make_unique<ASTLiteral>((*block.safeGetByPosition(0).column)[0]);
        lit->alias = subquery.alias;
        lit->prefer_alias_to_column_name = subquery.prefer_alias_to_column_name;
        ast = addTypeConversion(std::move(lit), block.safeGetByPosition(0).type->getName());
    }
    else
    {
        auto tuple = std::make_shared<ASTFunction>();
        tuple->alias = subquery.alias;
        ast = tuple;
        tuple->name = "tuple";
        auto exp_list = std::make_shared<ASTExpressionList>();
        tuple->arguments = exp_list;
        tuple->children.push_back(tuple->arguments);

        exp_list->children.resize(columns);
        for (size_t i = 0; i < columns; ++i)
        {
            exp_list->children[i] = addTypeConversion(
                std::make_unique<ASTLiteral>((*block.safeGetByPosition(i).column)[0]),
                block.safeGetByPosition(i).type->getName());
        }
    }
}

std::vector<ASTPtr *> ExecuteScalarSubqueriesMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data &)
{
    /// Don't descend into subqueries in arguments of IN operator.
    /// But if an argument is not subquery, than deeper may be scalar subqueries and we need to descend in them.

    std::vector<ASTPtr *> out;
    if (functionIsInOrGlobalInOperator(func.name))
    {
        for (auto & child : ast->children)
        {
            if (child != func.arguments)
                out.push_back(&child);
            else
                for (size_t i = 0, size = func.arguments->children.size(); i < size; ++i)
                    if (i != 1 || !typeid_cast<ASTSubquery *>(func.arguments->children[i].get()))
                        out.push_back(&func.arguments->children[i]);
        }
    }
    else
        for (auto & child : ast->children)
            out.push_back(&child);

    return out;
}

}
