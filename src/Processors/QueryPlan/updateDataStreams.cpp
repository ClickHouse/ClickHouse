#include <Processors/QueryPlan/QueryPlanVisitor.h>
#include "Processors/QueryPlan/ITransformingStep.h"

namespace DB
{

// constexpr bool debug_logging_enabled = false;

// class UpdateDataStreams : public QueryPlanVisitor<UpdateDataStreams, debug_logging_enabled>
// {
// public:
//     explicit UpdateDataStreams(QueryPlan::Node * root_) : QueryPlanVisitor<UpdateDataStreams, debug_logging_enabled>(root_) { }

//     static bool visitTopDownImpl(QueryPlan::Node * /*current_node*/, QueryPlan::Node * /*parent_node*/)
//     {
//         return true;
//     }

//     static void visitBottomUpImpl(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
//     {
//         if (parent_node->children.size() != 1)
//             return;

//         chassert(current_node->step->hasOutputStream());

//         if (auto * parent_transform_step = dynamic_cast<ITransformingStep*>(parent_node->step.get()); parent_transform_step)
//             parent_transform_step->updateInputStream(current_node->step->getOutputStream());
//     }
// };
}
