#pragma once

#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Loads RESOURCE and WORKLOAD sql objects from configuration.
/// Similar to WorkloadEntityKeeperStorage but loads from config instead of ZooKeeper.
class WorkloadEntityConfigStorage : public WorkloadEntityStorageBase
{
public:
    explicit WorkloadEntityConfigStorage(const ContextPtr & global_context_);
    void loadEntities(const Poco::Util::AbstractConfiguration & config) override;

    std::string_view getName() const override { return "configuration"; }

private:
    OperationResult storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    OperationResult removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override;
};

}
