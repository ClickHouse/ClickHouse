#pragma once

#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <functional>
#include <memory>
#include <unordered_map>

namespace DB
{

class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
class IMergeTreeDataPartInfoForReader;

/// Class that represents Key or Index condition template.
template <class Cond>
class ConditionTemplate
{
    Cond generate(const ActionsDAG * substituted_dag, const ActionsDAG::Node * root) const;

    const Cond * lookupUnsubstituted() const;
    const Cond & setUnsubstituted(Cond && cond) const;

    const Cond * lookupSubstituted(const std::string & cache_key) const;
    const Cond & setSubstituted(const std::string & cache_key, Cond && cond) const;

    const Cond & generateForPartition(const MergeTreePartition & partition, const String & partition_id, bool is_projection_part) const;

public:
    using Factory = std::function<Cond(const ActionsDAG *, const ActionsDAG::Node *)>;
    using Transformer = std::function<void(Cond &)>;
    using Transformers = std::vector<Transformer>;
    using Ptr = std::shared_ptr<ConditionTemplate<Cond>>;

    ConditionTemplate(
        std::shared_ptr<ActionsDAGWithInversionPushDown> dag_,
        Factory factory_,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_,
        bool skip_folding_);

    /// Substitutes nothing.
    const Cond & generateUnsubstituted() const;

    /// The filter DAG this template was built from, i.e. the predicates that participated in index analysis.
    const ActionsDAGWithInversionPushDown * getFilterDAG() const { return dag.get(); }

    /// Substitutes partition level constants into dag.
    const Cond & generateForPart(const MergeTreeDataPartPtr & part) const;
    const Cond & generateForPart(const IMergeTreeDataPartInfoForReader & part_info) const;

    /// Maps already generated condition using provided lambda.
    void addTransformation(Transformer transformer_);

private:
    const std::shared_ptr<ActionsDAGWithInversionPushDown> dag;
    const Factory factory;
    const StorageMetadataPtr metadata_snapshot;
    const ContextPtr context;
    const bool skip_folding;

    mutable std::mutex mutex;
    mutable std::optional<Cond> unsubstituted;
    mutable std::unordered_map<std::string, Cond> cache;
    mutable Transformers transformers;
};

}
