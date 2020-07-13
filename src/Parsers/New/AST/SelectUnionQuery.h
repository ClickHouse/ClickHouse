#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class SelectStmt;

class SelectUnionQuery : public Query
{
    public:
        void appendSelect(PtrTo<SelectStmt> stmt);

        ASTPtr convertToOld() const override;

    private:
        std::list<PtrTo<SelectStmt>> selects;
};

}
