#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class SelectUnionQuery : public Query
{
    public:
        void appendSelect(PtrTo<SelectStmt> stmt);
        void shouldBeScalar() { is_scalar = true; }

        ASTPtr convertToOld() const override;

    private:
        bool is_scalar = false;
};

}
