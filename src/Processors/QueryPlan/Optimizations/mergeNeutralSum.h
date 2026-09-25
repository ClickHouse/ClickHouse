#pragma once

#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// A single consumer observes this read only through grouping keys and a nullable
/// `sum` argument. Names are mapped to physical columns before storing the proof.
struct NeutralSumProof
{
    String measure;
    Names keys;
};

bool canCrossNeutralReduction(const ActionsDAG & actions);
Names neutralSumInputs(const ActionsDAG::Node & root);
void collectNeutralSumProofs(QueryPlan::Node & root);

}
