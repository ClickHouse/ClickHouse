#include <Storages/MergeTree/ConditionTemplate.h>

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
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VirtualColumnsDescription.h>

#include <base/defines.h>

#include <unordered_map>
#include <unordered_set>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

const std::string PARTITION_ID_NAME = "_partition_id";
const std::string PARTITION_VALUE_NAME = "_partition_value";

std::vector<const ActionsDAG::Node *> collectReachableNodesPostOrder(
    const ActionsDAG::Node * predicate_node)
{
    std::unordered_set<const ActionsDAG::Node *> observed;
    observed.insert(predicate_node);
    std::vector<std::pair<const ActionsDAG::Node *, size_t>> stack;
    stack.emplace_back(predicate_node, 0);

    std::vector<const ActionsDAG::Node *> order;
    while (!stack.empty())
    {
        const auto * node = stack.back().first;
        const size_t idx = stack.back().second;

        if (idx < node->children.size())
        {
            stack.back().second = idx + 1;

            const auto * child = node->children[idx];
            const bool not_observed = observed.insert(child).second;
            if (not_observed)
                stack.emplace_back(child, 0);

            continue;
        }

        order.push_back(node);
        stack.pop_back();
    }

    return order;
}

ColumnPtr makeConstantColumnForSubstitution(
    const String & name,
    const MergeTreePartition & partition,
    const String & partition_id,
    const StorageMetadataPtr & metadata_snapshot)
{
    if (name == PARTITION_ID_NAME)
    {
        auto column = metadata_snapshot->virtuals.get(PARTITION_ID_NAME, VirtualsKind::All, VirtualsMaterializationPlace::Reader);
        return column.type->createColumnConst(1, Field(partition_id));
    }

    if (name == PARTITION_VALUE_NAME)
    {
        Tuple tuple;
        tuple.reserve(partition.value.size());
        for (const auto & v : partition.value)
            tuple.push_back(v);

        auto column = metadata_snapshot->virtuals.get(PARTITION_VALUE_NAME, VirtualsKind::All, VirtualsMaterializationPlace::Reader);
        return column.type->createColumnConst(1, Field(std::move(tuple)));
    }

    for (const auto [column, value] : std::views::zip(metadata_snapshot->getPartitionKey().sample_block, partition.value))
        if (column.name == name)
            return column.type->createColumnConst(1, value);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported constant generation for column: {}", name);
}

NameSet collectPartitionConstantColumnNames(
    const StorageMetadataPtr & metadata_snapshot)
{
    NameSet names;

    if (metadata_snapshot->hasPartitionKey())
    {
        for (const auto & column : metadata_snapshot->getPartitionKey().sample_block)
            names.insert(column.name);

        if (metadata_snapshot->isVirtualColumn(PARTITION_ID_NAME))
            names.insert(PARTITION_ID_NAME);

        if (metadata_snapshot->isVirtualColumn(PARTITION_VALUE_NAME))
            names.insert(PARTITION_VALUE_NAME);
    }

    return names;
}

ActionsDAG substituteConstantInputs(
    const ActionsDAG::Node * predicate_node,
    const NameSet & inputs_to_substitute,
    const MergeTreePartition & partition,
    const String & partition_id,
    const StorageMetadataPtr & metadata_snapshot)
{
    chassert(predicate_node);

    ActionsDAG new_dag;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> mapping;

    for (const auto * old_node : collectReachableNodesPostOrder(predicate_node))
    {
        const ActionsDAG::Node * new_node = nullptr;

        switch (old_node->type)
        {
            case ActionsDAG::ActionType::INPUT:
            {
                if (inputs_to_substitute.contains(old_node->result_name))
                {
                    auto column = makeConstantColumnForSubstitution(old_node->result_name, partition, partition_id, metadata_snapshot);
                    new_node = &new_dag.addColumn(ColumnWithTypeAndName{column, old_node->result_type, old_node->result_name});
                }
                else
                {
                    new_node = &new_dag.addInput(old_node->result_name, old_node->result_type);
                }
                break;
            }
            case ActionsDAG::ActionType::COLUMN:
            {
                new_node = &new_dag.addColumn(ColumnWithTypeAndName{old_node->column, old_node->result_type, old_node->result_name}, old_node->is_deterministic_constant);
                break;
            }
            case ActionsDAG::ActionType::ALIAS:
            {
                new_node = &new_dag.addAlias(*mapping.at(old_node->children[0]), old_node->result_name);
                break;
            }
            case ActionsDAG::ActionType::FUNCTION:
            {
                ActionsDAG::NodeRawConstPtrs children;
                children.reserve(old_node->children.size());
                for (const auto * child : old_node->children)
                    children.push_back(mapping.at(child));

                new_node = &new_dag.addFunction(old_node->function_base, std::move(children), old_node->result_name);
                break;
            }
            case ActionsDAG::ActionType::ARRAY_JOIN:
            {
                new_node = &new_dag.addArrayJoin(*mapping.at(old_node->children[0]), old_node->result_name);
                break;
            }
            case ActionsDAG::ActionType::PLACEHOLDER:
            {
                new_node = &new_dag.addPlaceholder(old_node->result_name, old_node->result_type);
                break;
            }
        }

        mapping.emplace(old_node, new_node);
    }

    new_dag.getOutputs().push_back(mapping.at(predicate_node));
    return new_dag;
}

}

template <typename Cond>
ConditionTemplate<Cond>::ConditionTemplate(
    std::shared_ptr<ActionsDAGWithInversionPushDown> dag_,
    Factory factory_,
    StorageMetadataPtr metadata_snapshot_,
    ContextPtr context_)
    : dag(std::move(dag_))
    , factory(std::move(factory_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(context_))
{
    partition_constant_names = collectPartitionConstantColumnNames(metadata_snapshot);
}

template <typename Cond>
const Cond & ConditionTemplate<Cond>::generateUnsubstituted() const
{
    std::unique_lock lock(mutex);
    if (unsubstituted.has_value())
        return unsubstituted.value();

    const ActionsDAG::Node * predicate = dag ? dag->predicate : nullptr;
    Cond produced = factory(predicate);
    unsubstituted.emplace(std::move(produced));

    return unsubstituted.value();
}

template <typename Cond>
const Cond & ConditionTemplate<Cond>::generateForPartition(const MergeTreePartition & partition) const
{
    if (!dag || !dag->predicate)
        return generateUnsubstituted();

    std::unique_lock lock(mutex);

    const String partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (auto it = cache.find(partition_id); it != cache.end())
        return it->second;

    auto specialized = substituteConstantInputs(dag->predicate, partition_constant_names, partition, partition_id, metadata_snapshot);
    chassert(!specialized.getOutputs().empty());

    Cond produced = factory(specialized.getOutputs().front());
    const auto [it, inserted] = cache.emplace(partition_id, std::move(produced));
    chassert(inserted);

    return it->second;
}

template class ConditionTemplate<KeyCondition>;
template class ConditionTemplate<MergeTreeIndexConditionPtr>;

}
