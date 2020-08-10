#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class SetQuery : public Query
{
    public:
        explicit SetQuery(PtrTo<SettingExpr> expr);

        ASTPtr convertToOld() const override;
};

}
