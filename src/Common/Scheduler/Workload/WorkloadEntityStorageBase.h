#pragma once

#include <unordered_map>
#include <mutex>

#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST.h>

namespace DB
{

class WorkloadEntityStorageBase : public IWorkloadEntityStorage
{
public:
    explicit WorkloadEntityStorageBase(ContextPtr global_context_);
    ASTPtr get(const String & entity_name) const override;

    ASTPtr tryGet(const String & entity_name) const override;

    bool has(const String & entity_name) const override;

    std::vector<String> getAllEntityNames() const override;

    std::vector<std::pair<String, ASTPtr>> getAllEntities() const override;

    bool empty() const override;

    bool storeEntity(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    bool removeEntity(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override;

protected:
    virtual bool storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    virtual bool removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) = 0;

    std::unique_lock<std::recursive_mutex> getLock() const;
    void setAllEntities(const std::vector<std::pair<String, ASTPtr>> & new_entities);
    void setEntity(const String & entity_name, const IAST & create_entity_query);
    void removeEntity(const String & entity_name);
    void removeAllEntitiesExcept(const Strings & entity_names_to_keep);

    std::unordered_map<String, ASTPtr> entities; // Maps entity name into CREATE entity query
    mutable std::recursive_mutex mutex;

    ContextPtr global_context;
};

}
