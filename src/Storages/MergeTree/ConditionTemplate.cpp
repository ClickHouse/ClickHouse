#include <Storages/MergeTree/ConditionTemplate.h>

#include <Interpreters/ExpressionActions.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VirtualColumnsDescription.h>

#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

#include <base/defines.h>

#include <unordered_map>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void fillPartitionConstantsSubstitution(
    std::unordered_map<const ActionsDAG::Node *, ColumnWithTypeAndName> & substitutions,
    const ActionsDAG & predicate_dag,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreePartition & partition)
{
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    const auto & key_dag = partition_key.expression->getActionsDAG();
    const auto key_outputs = key_dag.findInOutputs(partition_key.column_names);
    const auto matches = matchTrees(key_outputs, predicate_dag, /*check_monotonicity=*/false);
    const auto partition_constants = std::views::zip(key_outputs, partition.value) | std::ranges::to<std::unordered_map<const ActionsDAG::Node *, Field>>();

    for (const auto & node : predicate_dag.getNodes())
    {
        const auto it = matches.find(&node);
        if (it == matches.end() || !it->second.node || it->second.monotonicity)
            continue;

        const auto [_, match] = *it;
        if (!partition_constants.contains(match.node))
            continue;

        auto column = node.result_type->createColumnConst(1, partition_constants.at(match.node));
        substitutions.emplace(&node, ColumnWithTypeAndName{column, node.result_type, node.result_name});
    }
}

void fillVirtualConstantsSubstitution(
    std::unordered_map<const ActionsDAG::Node *, ColumnWithTypeAndName> & substitutions,
    const ActionsDAG & predicate_dag,
    const StorageMetadataPtr & metadata_snapshot,
    const std::string & partition_id,
    const MergeTreePartition & partition)
{
    const auto add_virtual = [&](const String & name, const Field & value)
    {
        if (!metadata_snapshot->isVirtualColumn(name))
            return;

        const auto column_desc = metadata_snapshot->virtuals.get(name, VirtualsKind::All, VirtualsMaterializationPlace::All);
        for (const auto & node : predicate_dag.getNodes())
        {
            if (node.type != ActionsDAG::ActionType::INPUT || node.result_name != name)
                continue;
            if (substitutions.contains(&node))
                continue;

            auto column = column_desc.type->createColumnConst(1, value);
            substitutions.emplace(&node, ColumnWithTypeAndName{column, column_desc.type, node.result_name});
        }
    };

    add_virtual(PartitionIdColumn::name, Field(partition_id));
    add_virtual(PartitionValueColumn::name, partition.value | std::ranges::to<Tuple>());
}

ActionsDAG substituteConstantInputs(
    const ActionsDAG::Node * predicate_node,
    const MergeTreePartition & partition,
    const String & partition_id,
    const StorageMetadataPtr & metadata_snapshot)
{
    chassert(predicate_node);

    auto dag = ActionsDAG::cloneSubDAG({predicate_node}, /*remove_aliases=*/false);

    std::unordered_map<const ActionsDAG::Node *, ColumnWithTypeAndName> substitutions;
    fillPartitionConstantsSubstitution(substitutions, dag, metadata_snapshot, partition);
    fillVirtualConstantsSubstitution(substitutions, dag, metadata_snapshot, partition_id, partition);

    dag.substitute(substitutions);
    dag.removeUnusedActions(/*allow_remove_inputs=*/false, /*allow_constant_folding=*/true, /*evaluate_constants=*/true);

    return dag;
}

}

template <typename Cond>
Cond ConditionTemplate<Cond>::generate(const ActionsDAG * substituted_dag, const ActionsDAG::Node * root) const
{
    Cond condition = factory(substituted_dag, root);

    for (const auto & transformer : transformers)
        transformer(condition);

    return condition;
}

template <typename Cond>
ConditionTemplate<Cond>::ConditionTemplate(
    std::shared_ptr<ActionsDAGWithInversionPushDown> dag_,
    Factory factory_,
    StorageMetadataPtr metadata_snapshot_,
    ContextPtr context_,
    bool skip_folding_)
    : dag(std::move(dag_))
    , factory(std::move(factory_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(context_))
    , skip_folding(skip_folding_)
{
    generateUnsubstituted();
}

template <typename Cond>
const Cond & ConditionTemplate<Cond>::generateUnsubstituted() const
{
    std::unique_lock lock(mutex);
    if (unsubstituted.has_value())
        return unsubstituted.value();

    const ActionsDAG * unsubsituted = dag && dag->dag.has_value() ? &dag->dag.value() : nullptr;
    const ActionsDAG::Node * predicate = dag ? dag->predicate : nullptr;
    Cond produced = generate(unsubsituted, predicate);
    unsubstituted.emplace(std::move(produced));

    return unsubstituted.value();
}

template <typename Cond>
const Cond & ConditionTemplate<Cond>::generateForPartition(const MergeTreePartition & partition) const
{
    if (skip_folding || !dag || !dag->predicate)
        return generateUnsubstituted();

    std::unique_lock lock(mutex);

    const String partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (auto it = cache.find(partition_id); it != cache.end())
        return it->second;

    try
    {
        auto specialized = substituteConstantInputs(dag->predicate, partition, partition_id, metadata_snapshot);
        chassert(!specialized.getOutputs().empty());

        Cond produced = generate(&specialized, specialized.getOutputs().front());
        const auto [it, inserted] = cache.emplace(partition_id, std::move(produced));
        chassert(inserted);

        return it->second;
    }
    catch (...) /// Ok. Substitution is done in best-effort way.
    {
        lock.unlock();
        return generateUnsubstituted();
    }
}

template <typename Cond>
void ConditionTemplate<Cond>::addTransformation(Transformer transformer_)
{
    std::unique_lock lock(mutex);

    unsubstituted.reset();
    cache.clear();

    transformers.push_back(std::move(transformer_));
}

template class ConditionTemplate<KeyCondition>;
template class ConditionTemplate<MergeTreeIndexConditionPtr>;

}
