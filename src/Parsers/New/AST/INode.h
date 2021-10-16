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
        virtual String toString() const { return {}; }

        void dump() const { dump(0); }

    protected:
        INode() = default;
        INode(std::initializer_list<Ptr> list) { children = list; }
        explicit INode(PtrList list) { children = list; }
        explicit INode(size_t size) { children.resize(size); }

        void push(const Ptr& child) { children.push_back(child); }
        void set(size_t i, const Ptr& child) { children[i] = child; }
        bool has(size_t i) const { return i < children.size() && children[i]; }
        const Ptr & get(size_t i) const { return children[i]; }

        template <class ChildType>
        bool has(size_t i) const { return has(i) && children[i]->as<ChildType>(); }

        template <class ChildType>
        ChildType * get(size_t i) const { return children[i]->template as<ChildType>(); }

        auto begin() const { return children.cbegin(); }
        auto end() const { return children.cend(); }
        auto size() const { return children.size(); }

    private:
        PtrList children;  // any child potentially may point to |nullptr|

        void dump(int indentation) const
        {
            for (auto i = 0; i < indentation; ++i) std::cout << " ";
            std::cout << "⭸ " << demangle(typeid(*this).name()) << " (" << dumpInfo() << ")" << std::endl;
            for (const auto & child : children) if (child) child->dump(indentation + 1);
        }

        virtual String dumpInfo() const { return ""; }
};

template <class T, char Separator>
class List : public INode {
    public:
        List() = default;
        List(std::initializer_list<PtrTo<T>> list)
        {
            for (const auto & i : list) push(i);
        }

        using INode::begin;
        using INode::end;
        using INode::size;

        void push(const PtrTo<T> & node) { INode::push(node); }

        ASTPtr convertToOld() const override
        {
            auto list = std::make_shared<ASTExpressionList>(Separator);
            for (const auto & child : *this) list->children.emplace_back(child->convertToOld());
            return list;
        }

        String toString() const override
        {
            if (!size()) return {};

            auto string = (*begin())->toString();

            for (auto next = ++begin(); next != end(); ++next)
                string += String(1, Separator) + " " + (*next)->toString();

            return string;
        }
};

template <class T>
class SimpleClause : public INode
{
    public:
        explicit SimpleClause(PtrTo<T> expr) : INode{expr} {}
        ASTPtr convertToOld() const override { return get(0)->convertToOld(); }
};

}
