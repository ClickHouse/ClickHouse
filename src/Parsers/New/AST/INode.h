#pragma once

#include <Common/TypePromotion.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <vector>


namespace DB::AST {

class INode;

template <class T = INode>
using PtrTo = std::shared_ptr<T>;

using Ptr = PtrTo<>;

class INode : public TypePromotion<INode>
{
    public:
        virtual ~INode() = default;
        virtual ASTPtr convertToOld() const { return ASTPtr(); }

    protected:
        std::vector<Ptr> children;
};

template <class T, char Separator>
class List : public INode {
    public:
        void append(PtrTo<T> node) { children.push_back(node); }

        auto begin() const { return children.cbegin(); }
        auto end() const { return children.cend(); }
};

}
