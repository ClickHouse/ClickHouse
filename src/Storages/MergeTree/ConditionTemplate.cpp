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

#include <mutex>
#include <unordered_map>
#include <ranges>

namespace DB
{

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

    for (const auto & [node, match] : matches)
    {
        if (!match.node || match.monotonicity)
            continue;

        const auto it = partition_constants.find(match.node);
        if (it == partition_constants.end())
            continue;

        auto column = node->result_type->createColumnConst(1, it->second);
        substitutions.emplace(node, ColumnWithTypeAndName{column->getPtr(), node->result_type, node->result_name});
    }
}

void fillVirtualConstantsSubstitution(
    std::unordered_map<const ActionsDAG::Node *, ColumnWithTypeAndName> & substitutions,
    const ActionsDAG & predicate_dag,
    const StorageMetadataPtr & metadata_snapshot,
    const std::string & partition_id,
    const MergeTreePartition & partition)
{
    const auto add_virtual = [&](const std::string & name, const Field & value)
    {
        if (!metadata_snapshot->isVirtualColumn(name))
            return;

        const auto column_desc = metadata_snapshot->virtuals.get(name, VirtualsKind::All, VirtualsMaterializationPlace::All);
        for (const auto * node : predicate_dag.getInputs())
        {
            if (node->result_name != name || substitutions.contains(node))
                continue;

            auto column = column_desc.type->createColumnConst(1, value);
            substitutions.emplace(node, ColumnWithTypeAndName{column->getPtr(), column_desc.type, node->result_name});
        }
    };

    add_virtual(PartitionIdColumn::name, Field(partition_id));
    add_virtual(PartitionValueColumn::name, partition.value | std::ranges::to<Tuple>());
}

ActionsDAG substituteConstantInputs(
    const ActionsDAG::Node * predicate_node,
    const MergeTreePartition & partition,
    const std::string & partition_id,
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

    for (const auto & transform : transformers)
        transform(condition);

    return condition;
}

template <typename Cond>
const Cond * ConditionTemplate<Cond>::lookupUnsubstituted() const
{
    std::unique_lock lock(mutex);

    if (unsubstituted.has_value())
        return &unsubstituted.value();

    return nullptr;
}

template <typename Cond>
const Cond & ConditionTemplate<Cond>::setUnsubstituted(Cond && cond) const
{
    std::unique_lock lock(mutex);

    if (!unsubstituted.has_value())
        unsubstituted.emplace(std::forward<Cond>(cond));

    return unsubstituted.value();
}

template <typename Cond>
const Cond * ConditionTemplate<Cond>::lookupSubstituted(const std::string & cache_key) const
{
    std::unique_lock lock(mutex);

    if (auto it = cache.find(cache_key); it != cache.end())
        return &it->second;

    return nullptr;
}

template <typename Cond>
const Cond & ConditionTemplate<Cond>::setSubstituted(const std::string & cache_key, Cond && cond) const
{
    std::unique_lock lock(mutex);

    const auto [it, _] = cache.emplace(cache_key, std::forward<Cond>(cond));

    return it->second;
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
    if (const auto * cond = lookupUnsubstituted())
        return *cond;

    const ActionsDAG * unsubsituted = dag && dag->dag.has_value() ? &dag->dag.value() : nullptr;
    const ActionsDAG::Node * predicate = dag ? dag->predicate : nullptr;
    Cond produced = generate(unsubsituted, predicate);
    return setUnsubstituted(std::move(produced));
}

template <typename Cond>
const Cond & ConditionTemplate<Cond>::generateForPartition(const MergeTreePartition & partition) const
{
    if (skip_folding || !dag || !dag->predicate)
        return generateUnsubstituted();

    const std::string partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (const auto * cond = lookupSubstituted(partition_id))
        return *cond;

    try
    {
        auto specialized = substituteConstantInputs(dag->predicate, partition, partition_id, metadata_snapshot);
        chassert(!specialized.getOutputs().empty());

        Cond produced = generate(&specialized, specialized.getOutputs().front());
        return setSubstituted(partition_id, std::move(produced));
    }
    catch (const Exception &)
    {
        /// Constant substitution is best-effort: only expected query-evaluation failures
        /// (e.g. division by zero while folding a partition constant) are caught here.
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
