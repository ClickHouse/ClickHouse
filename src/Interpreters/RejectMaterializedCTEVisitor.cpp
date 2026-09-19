#include <Interpreters/RejectMaterializedCTEVisitor.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool force_materialized_cte;
}

bool shouldRejectMaterializedCTE(const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::force_materialized_cte])
        return false;
    auto txn = context->getZooKeeperMetadataTransaction();
    return !txn || txn->isInitialQuery();
}

}
