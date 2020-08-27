#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class SettingExpr : public INode
{
    public:
        SettingExpr(PtrTo<Identifier> name, PtrTo<Literal> value);

        auto getName() const { return static_pointer_cast<Identifier>(children[NAME]); }
        auto getValue() const { return static_pointer_cast<Literal>(children[VALUE]); }

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            VALUE = 1,
        };
};

}
