#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class SettingExpr : public INode
{
    public:
        SettingExpr(PtrTo<Identifier> name_, PtrTo<Literal> value_);

        auto getName() const { return name; }
        auto getValue() const { return value; }

    private:
        PtrTo<Identifier> name;
        PtrTo<Literal> value;
};

}
