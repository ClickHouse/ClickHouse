#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

class ApplyWithColumnNamesVisitor
{
public:
    static void visit(ASTPtr & ast);

private:
    static void visit(ASTSelectQuery & ast);
};

}
