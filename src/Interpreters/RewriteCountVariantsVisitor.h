#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTFunction;

class RewriteCountVariantsVisitor
{
public:
    static void visit(ASTPtr &);
    static void visit(ASTFunction &);
};

}
