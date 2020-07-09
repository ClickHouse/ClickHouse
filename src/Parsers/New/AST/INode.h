#pragma once

#include <list>
#include <memory>

namespace DB::AST {

class INode;

template <class T = INode>
using PtrTo = std::shared_ptr<T>;

using Ptr = PtrTo<>;

class INode {
    protected:
        std::list<Ptr> children;
};

template <class T, char Separator>
class List : public INode {
    public:
        void append(PtrTo<T> node);

        auto begin() const { return children.cbegin(); }
        auto end() const { return children.cend(); }
};

}
