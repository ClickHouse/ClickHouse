#pragma once

#include <Parsers/New/AST/fwd_decl.h>

#include <Common/TypePromotion.h>
#include <Parsers/ASTExpressionList.h>


namespace DB::AST
{

class INode : public TypePromotion<INode>
{
    public:
        virtual ~INode() = default;
        virtual ASTPtr convertToOld() const { return ASTPtr(); }

    protected:
        PtrList children;
};

template <class T, char Separator>
class List : public INode {
    public:
        void append(PtrTo<T> node) { children.push_back(node); }

        auto begin() const { return children.cbegin(); }
        auto end() const { return children.cend(); }

        ASTPtr convertToOld() const override
        {
            auto list = std::make_shared<ASTExpressionList>();
            for (const auto & child : *this) list->children.emplace_back(child->convertToOld());
            return list;
        }
};

}
