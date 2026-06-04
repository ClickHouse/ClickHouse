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

/// Class that represents Key or Index condition template.
template <class Cond>
class ConditionTemplate
{
    Cond generate(const ActionsDAG * substituted_dag, const ActionsDAG::Node * root) const;

public:
    using Factory = std::function<Cond(const ActionsDAG *, const ActionsDAG::Node *)>;
    using Transformer = std::function<void(Cond &)>;
    using Ptr = std::shared_ptr<ConditionTemplate<Cond>>;

    ConditionTemplate(
        std::shared_ptr<ActionsDAGWithInversionPushDown> dag_,
        Factory factory_,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_,
        bool skip_folding_);

    /// Substitutes nothing.
    const Cond & generateUnsubstituted() const;

    /// Substitutes partition level constants into dag.
    const Cond & generateForPartition(const MergeTreePartition & partition) const;

    /// Maps already generated condition using provided lambda.
    void addTransformation(Transformer transformer_);

private:
    std::shared_ptr<ActionsDAGWithInversionPushDown> dag;
    Factory factory;
    Transformer transformer;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    bool skip_folding;

    mutable std::mutex mutex;
    mutable std::optional<Cond> unsubstituted;
    mutable std::unordered_map<String, Cond> cache;
};

}
