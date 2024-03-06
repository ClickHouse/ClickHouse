#include <Interpreters/Context.h>
#include <stack>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryCoordination/Fragments/FragmentBuilder.h>

namespace DB
{

FragmentBuilder::FragmentBuilder(QueryPlan & plan_, ContextMutablePtr context_) : plan(plan_), context(context_)
{
}

FragmentPtr FragmentBuilder::build()
{
    struct Frame
    {
        PlanNode * node = {};
        FragmentPtrs child_fragments = {};
    };

    PlanNode * root = plan.getRootNode();

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    FragmentPtr last_fragment;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_fragment)
        {
            frame.child_fragments.emplace_back(std::move(last_fragment));
            last_fragment = nullptr;
        }

        size_t next_child = frame.child_fragments.size();
        if (next_child == frame.node->children.size())
        {
            if (auto * exchange_step = typeid_cast<ExchangeDataStep *>(frame.node->step.get()))
            {
                last_fragment = std::make_shared<Fragment>(context->getFragmentID(), context);
                last_fragment->addStep(frame.node->step);

                exchange_step->setFragmentId(last_fragment->getFragmentID());
                exchange_step->setPlanID(last_fragment->getRoot()->plan_id);
                frame.child_fragments[next_child - 1]->setDestination(last_fragment->getRoot(), last_fragment);
            }
            else if (next_child == 0)
            {
                last_fragment = std::make_shared<Fragment>(context->getFragmentID(), context);
                last_fragment->addStep(frame.node->step);
            }
            else if (next_child == 1)
            {
                frame.child_fragments[0]->addStep(frame.node->step);
                last_fragment = frame.child_fragments[0];
            }
            else
            {
                last_fragment = std::make_shared<Fragment>(context->getFragmentID(), context);
                last_fragment->uniteFragments(frame.node->step, frame.child_fragments);
            }

            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    return last_fragment;
}

}
