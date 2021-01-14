#pragma once

#include <Parsers/IAST.h>

namespace DB
{
/// Pull out the WITH statement from the first child of ASTSelectWithUnion query if any.
class ApplyWithGlobalVisitor
{
public:
    static void visit(ASTPtr & ast);
};

}
