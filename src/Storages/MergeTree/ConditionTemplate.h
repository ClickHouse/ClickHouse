#pragma once

#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/KeyCondition.h>
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
public:
    using Factory = std::function<Cond(const ActionsDAG::Node *)>;
    using Ptr = std::shared_ptr<ConditionTemplate<Cond>>;

    ConditionTemplate(
        std::shared_ptr<ActionsDAGWithInversionPushDown> dag_,
        Factory factory_,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_);

    const ActionsDAG * getDAG() const { return &dag->dag.value(); }

    /// Substitutes partition level constants into dag.
    const Cond & generateForPart(const IMergeTreeDataPart & part) const;

    /// Substitutes nothing.
    const Cond & generateUnsubstituted() const;

private:
    std::shared_ptr<ActionsDAGWithInversionPushDown> dag;
    Factory factory;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    NameSet partition_constant_names;

    mutable std::mutex mutex;
    mutable std::optional<Cond> unsubstituted;
    mutable std::unordered_map<String, Cond> cache;
};

}
