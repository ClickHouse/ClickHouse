#include <Storages/System/StorageSystemQuotasUsage.h>
#include <Storages/System/StorageSystemQuotaUsage.h>
#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/QuotaUsage.h>


namespace DB
{
ColumnsDescription StorageSystemQuotasUsage::getColumnsDescription()
{
    return StorageSystemQuotaUsage::getColumnsDescriptionImpl(/* add_column_is_current = */ true);
}

void StorageSystemQuotasUsage::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_QUOTAS);

    auto all_quotas_usage = context->getAccessControl().getAllQuotasUsage();
    StorageSystemQuotaUsage::fillDataImpl(res_columns, context, /* add_column_is_current = */ true, all_quotas_usage);
}
}
