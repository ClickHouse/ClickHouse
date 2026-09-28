#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Poco/Logger.h>

namespace DB
{
template <typename Derived, bool debug_logging = false>
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

            logStep("back", current_node);

            /// top-down visit
            if (0 == frame.next_child)
            {
                logStep("top-down", current_node);
                if (!visitTopDown(current_node, parent_node))
                    continue;
            }
            /// Traverse all children
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = FrameWithParent{.node = current_node->children[frame.next_child], .parent_node = current_node};
                ++frame.next_child;
                logStep("push", next_frame.node);
                stack.push_back(next_frame);
                continue;
            }

            /// bottom-up visit
            logStep("bottom-up", current_node);
            visitBottomUp(current_node, parent_node);

            logStep("pop", current_node);
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

    std::unordered_map<const IQueryPlanStep*, std::string> address2name;
    std::unordered_map<std::string, UInt32> name_gen;

    std::string getStepId(const IQueryPlanStep* step)
    {
        const auto step_name = step->getName();
        auto it = address2name.find(step);
        if (it != address2name.end())
            return it->second;

        const auto seq_num = name_gen[step_name]++;
        return address2name.insert({step, fmt::format("{}{}", step_name, seq_num)}).first->second;
    }

protected:
    void logStep(const char * prefix, const QueryPlan::Node * node)
    {
        if constexpr (debug_logging)
        {
            const IQueryPlanStep * current_step = node->step.get();
            LOG_DEBUG(
                getLogger("QueryPlanVisitor"),
                "{}: {}: {}",
                prefix,
                getStepId(current_step),
                reinterpret_cast<const void *>(current_step));
        }
    }
};

}
