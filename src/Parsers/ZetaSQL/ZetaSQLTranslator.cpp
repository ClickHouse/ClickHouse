#include "Parsers/ZetaSQL/ZetaSQLTranslator.h"
#include <memory>
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTSelectWithUnionQuery.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/ZetaSQL/ASTZetaSQLQuery.h"
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTAsterisk.h>
#include "Common/Exception.h"

namespace DB::ZetaSQL
{
    /*
        Receives a valid ZetaSQL AST and translates it to a corresponding SELECT statement
        for ClickhouseSQL
    */
    ASTPtr Translator::translateAST(ASTZetaSQLQuery & zetasql_ast)
    {
        auto select_ast = std::make_shared<ASTSelectQuery>();
        select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
        select_ast->select()->children.push_back(std::make_shared<ASTAsterisk>());

        for( auto & stage: zetasql_ast.stages)
        {
            switch(stage.type)
            {
                case ASTZetaSQLQuery::StageKeyword::FROM:
                    processFromStage(select_ast, stage);
                    break;
                case ASTZetaSQLQuery::StageKeyword::WHERE:
                    processWhereStage(select_ast, stage);
                    break;
                case ASTZetaSQLQuery::StageKeyword::SELECT:
                    throw Exception::createDeprecated("TODO", 1);
                case ASTZetaSQLQuery::StageKeyword::AGGREGATE:
                    throw Exception::createDeprecated("TODO", 1);
            }
        }

        auto clickhouse_ast =  std::make_shared<ASTSelectWithUnionQuery>();
        auto list_of_selects = std::make_shared<ASTExpressionList>();
        list_of_selects->children.push_back(select_ast);
        clickhouse_ast->children.push_back(std::move(list_of_selects));
        clickhouse_ast->list_of_selects = clickhouse_ast->children.back();

        return clickhouse_ast;
    }

    void Translator::processFromStage(std::shared_ptr<ASTSelectQuery>  select_query, ASTZetaSQLQuery::PipelineStage & stage)
    {
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(stage.expression));
    }

    void Translator::processWhereStage(std::shared_ptr<ASTSelectQuery>  select_query, ASTZetaSQLQuery::PipelineStage & stage)
    {
        if(select_query->where()){
            auto and_function = std::make_shared<ASTFunction>();
            and_function->name = "and";

            auto args = std::make_shared<ASTExpressionList>();
            args->children = {select_query->where(), stage.expression};

            and_function->arguments = args;
            and_function->children.push_back(args);

            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(and_function));
        } else {
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(stage.expression));
        }
    }
}
