#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{
template <typename Derived>
class QueryPlanVisitor
{
protected:
    struct FrameWithParent
    {
        QueryPlan::Node * node = nullptr;
        QueryPlan::Node * parent_node = nullptr;
        size_t next_child = 0;
    };

    using StackWithParent = std::vector<FrameWithParent>;

    QueryPlan::Node * root = nullptr;
    StackWithParent stack;

public:
    explicit QueryPlanVisitor(QueryPlan::Node * root_) : root(root_) { }

    void visit()
    {
        stack.push_back({.node = root});

        while (!stack.empty())
        {
            auto & frame = stack.back();

            QueryPlan::Node * current_node = frame.node;
            QueryPlan::Node * parent_node = frame.parent_node;

            /// top-down visit
            if (0 == frame.next_child)
            {
                if (!visitTopDown(current_node, parent_node))
                    continue;
            }
            /// Traverse all children
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = FrameWithParent{.node = current_node->children[frame.next_child], .parent_node = current_node};
                ++frame.next_child;
                stack.push_back(next_frame);
                continue;
            }

            /// bottom-up visit
            visitBottomUp(current_node, parent_node);

            stack.pop_back();
        }
    }

    bool visitTopDown(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        return getDerived().visitTopDownImpl(current_node, parent_node);
    }
    void visitBottomUp(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
    {
        getDerived().visitBottomUpImpl(current_node, parent_node);
    }

private:
    Derived & getDerived() { return *static_cast<Derived *>(this); }

    const Derived & getDerived() const { return *static_cast<Derived *>(this); }
};

}
