#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class SettingExpr : public INode
{
    public:
        SettingExpr(PtrTo<Identifier> name_, PtrTo<Literal> value_);

    private:
        PtrTo<Identifier> name;
        PtrTo<Literal> value;
};

}
