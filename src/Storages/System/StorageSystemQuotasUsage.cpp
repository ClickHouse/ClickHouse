#include <Storages/System/StorageSystemQuotasUsage.h>
#include <Storages/System/StorageSystemQuotaUsage.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <Access/QuotaUsage.h>


namespace DB
{
NamesAndTypesList StorageSystemQuotasUsage::getNamesAndTypes()
{
    return StorageSystemQuotaUsage::getNamesAndTypesImpl(/* add_column_is_current = */ true);
}

void StorageSystemQuotasUsage::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_QUOTAS);
    auto all_quotas_usage = context.getAccessControlManager().getAllQuotasUsage();
    StorageSystemQuotaUsage::fillDataImpl(res_columns, context, /* add_column_is_current = */ true, all_quotas_usage);
}
}
