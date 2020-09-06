#pragma once

#include <Parsers/New/AST/fwd_decl.h>

#include <common/demangle.h>
#include <Common/TypePromotion.h>
#include <Parsers/ASTExpressionList.h>

#include <initializer_list>
#include <iostream>


namespace DB::AST
{

class INode : public TypePromotion<INode>
{
    public:
        virtual ~INode() = default;
        virtual ASTPtr convertToOld() const { return ASTPtr(); }

        void dump() const { dump(0); }

    protected:
        bool has(size_t i) const { return i < children.size() && children[i]; }

        PtrList children;  // any child potentially may point to |nullptr|

    private:
        void dump(int indentation) const
        {
            for (auto i = 0; i < indentation; ++i) std::cout << " ";
            std::cout << "⭸ " << dumpInfo() << " (" << demangle(typeid(*this).name()) << ")" << std::endl;
            for (const auto & child : children) if (child) child->dump(indentation + 1);
        }

        virtual String dumpInfo() const { return ""; }
};

template <class T, char Separator>
class List : public INode {
    public:
        List() = default;
        List(std::initializer_list<Ptr> list) { children = list; }

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
