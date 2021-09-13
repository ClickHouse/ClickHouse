#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class SetQuery : public Query
{
    public:
        explicit SetQuery(PtrTo<SettingExprList> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPRS = 0,  // SettingExprList
        };
};

}
