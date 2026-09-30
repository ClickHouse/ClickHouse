#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <cstdint>
#include <memory>

namespace DB
{

class ASTAlterQuery;
class ASTCreateQuery;
class IDatabase;
struct ProjectionsDescription;

/// Metadata provenance and DDL transport are separate from loading strictness. These helpers
/// decide whether session-dependent checks belong to the initiating operation or its replay.
enum class ProjectionDefinitionSource : uint8_t
{
    NewQuery,
    Backup,
    PreviouslyAccepted,
};

ProjectionDefinitionSource getProjectionDefinitionSource(
    LoadingStrictnessLevel mode, bool attach_short_syntax, bool is_restore_from_backup);
bool isInitialProjectionMetadataQuery(const ContextPtr & context);
bool isSecondaryProjectionMetadataReplay(const ContextPtr & context);
bool shouldValidateProjectionCodecsOnCreate(
    const ContextPtr & context,
    LoadingStrictnessLevel mode,
    bool attach_short_syntax,
    bool is_restore_from_backup);
bool shouldValidateProjectionCodecsOnAlter(const ContextPtr & context);

bool isProjectionStorageReplicated(const ASTCreateQuery & create);

/// Admit a declaration before it enters distributed metadata. Call this before the early
/// return for legacy `ON CLUSTER`, and again after `CREATE AS` has resolved its source metadata.
void validateProjectionMetadataAdmission(
    const ASTCreateQuery & create,
    const ContextPtr & context,
    const std::shared_ptr<IDatabase> & database,
    ProjectionDefinitionSource source,
    bool copies_source_projections,
    const ProjectionsDescription * copied_projections = nullptr);

void validateProjectionMetadataAdmission(
    const ASTAlterQuery & alter,
    const StoragePtr & table,
    const std::shared_ptr<IDatabase> & database,
    const ContextPtr & context);

void validateProjectionCodecOldDistributedDDLAdmission(const ASTAlterQuery & alter, const ContextPtr & context);

}
