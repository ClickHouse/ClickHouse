#include <Databases/LoadingStrictnessLevel.h>

#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <base/defines.h>

#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

namespace DB
{

LoadingStrictnessLevel getLoadingStrictnessLevel(bool attach, bool force_attach, bool force_restore, bool secondary)
{
    if (force_restore)
    {
        chassert(attach);
        chassert(force_attach);
        return LoadingStrictnessLevel::FORCE_RESTORE;
    }

    if (force_attach)
    {
        chassert(attach);
        return LoadingStrictnessLevel::FORCE_ATTACH;
    }

    if (attach)
        return LoadingStrictnessLevel::ATTACH;

    if (secondary)
        return LoadingStrictnessLevel::SECONDARY_CREATE;

    return LoadingStrictnessLevel::CREATE;
}

bool isReplayOfJudgedDefinition(const ContextPtr & context)
{
    const auto metadata_txn = context->getZooKeeperMetadataTransaction();
    if (metadata_txn && !metadata_txn->isInitialQuery())
        return true;

    if (context->isRecoveryFromStoredMetadata())
        return true;

#if CLICKHOUSE_CLOUD
    return context->getClientInfo().is_shared_catalog_internal && !SharedDatabaseCatalog::isInitialQuery(context);
#else
    return false;
#endif
}

}
