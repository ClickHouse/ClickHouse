#pragma once
#include <memory>
#include <ratio>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/PostgreSQL/Columns/TransformColumns.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>

namespace DB::PostgreSQL
{
    ASTPtr createMemoryAST() {
        auto function = std::make_shared<ASTFunction>();
        function->name = "Memory";
        auto arguments = std::make_shared<ASTExpressionList>();
        function->arguments = arguments;
        function->children.push_back(arguments);

        return function;
    }

    ASTPtr TransformCreateStatement(const std::shared_ptr<Node> node)
    {
        auto ast = std::make_shared<ASTCreateQuery>();
        if (node->HasChildWithKey("tableElts"))
        {
            ast->set(ast->columns_list, TransformColumns((*node)["tableElts"]));
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
        }
        auto storage = std::make_shared<ASTStorage>();
        storage->set(storage->engine, createMemoryAST());
        ast->setDatabase("default");
        if (node->HasChildWithKey("relation") && (*node)["relation"]->HasChildWithKey("relname")) {
            ast->setTable((*(*node)["relation"])["relname"]->GetStringValue());
        }
        ast->set(ast->storage, storage);

        return ast;
    }

}
