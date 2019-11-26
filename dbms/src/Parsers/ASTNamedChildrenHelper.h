#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTCreateQuery.h>

#include <unordered_map>

namespace DB
{

#define DECLARE_SELF_AND_CHILDREN(CLASS_NAME, ...) \
    CLASS_NAME; \
    enum CLASS_NAME##Children {__VA_ARGS__}; \
    class CLASS_NAME

template <typename Base, typename Derived, typename Children>
class ASTWithNamedChildren : public Base
{
public:
    const ASTPtr getChild(Children child_name) const
    {
        auto it = positions.find(child_name);

        if (it != positions.end())
            return Base::children[it->second];

        return {};
    }

    void setChild(Children child_name, ASTPtr & child)
    {
        if (child)
        {
            auto it = positions.find(child_name);
            if (it == positions.end())
            {
                positions[child_name] = Base::children.size();
                Base::children.emplace_back(child);
            }
            else
                Base::children[it->second] = child;
        }
        else if (positions.count(child_name))
        {
            size_t pos = positions[child_name];
            Base::children.erase(Base::children.begin() + pos);
            positions.erase(child_name);
            for (auto & pr : positions)
                if (pr.second > pos)
                    --pr.second;
        }
    }

private:
    std::unordered_map<Children, size_t> positions;
};


//template <typename Named>
//class NamedASTImpl
//{
//public:
//    NamedASTImpl(IAST * node_): node(node_) {}
//
//    void clone(const NamedASTImpl<Named> & other)
//    {
//        positions->clear();
//        for (auto & position : other.positions)
//            set(position->first, other.get(position->first)->clone());
//    }
//
//    const ASTPtr get(Named name) const
//    {
//        auto it = positions.find(name);
//        if (it != positions.end())
//            return node->children[it->second];
//        return {};
//    }
//
//    void set(Named name, ASTPtr & ast)
//    {
//        if (ast)
//        {
//            auto it = positions.find(name);
//            if (it == positions.end())
//            {
//                positions[name] = node->children.size();
//                node->children.emplace_back(ast);
//            }
//            else
//                node->children[it->second] = ast;
//        }
//        else if (positions.count(name))
//        {
//            size_t pos = positions[name];
//            node->children.erase(node->children.begin() + pos);
//            positions.erase(name);
//            for (auto & pr : positions)
//                if (pr.second > pos)
//                    --pr.second;
//        }
//    }
//
//private:
//    IAST * node;
//    std::unordered_map<Named, size_t> positions;
//};

}

