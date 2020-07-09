#pragma once

#include <Parsers/New/AST/INode.h>

#include <Parsers/IAST_fwd.h>

namespace DB::AST
{

class Query : public INode {
    public:
        ASTPtr convertToOld() const;
};

using QueryList = List<Query, ';'>;

}
