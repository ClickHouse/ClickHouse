#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class LimitExpr : public INode
{
    public:
        explicit LimitExpr(PtrTo<ColumnExpr> limit, PtrTo<ColumnExpr> offset = nullptr);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            LIMIT = 0,   // ColumnExpr
            OFFSET = 1,  // ColumnExpr (optional)
        };
};

}
