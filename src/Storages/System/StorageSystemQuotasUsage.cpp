#include <Storages/System/StorageSystemQuotasUsage.h>
#include <Storages/System/StorageSystemQuotaUsage.h>
#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/QuotaUsage.h>


namespace DB
{
NamesAndTypesList StorageSystemQuotasUsage::getNamesAndTypes()
{
    return StorageSystemQuotaUsage::getNamesAndTypesImpl(/* add_column_is_current = */ true);
}

void StorageSystemQuotasUsage::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_QUOTAS);
    auto all_quotas_usage = context->getAccessControl().getAllQuotasUsage();
    StorageSystemQuotaUsage::fillDataImpl(res_columns, context, /* add_column_is_current = */ true, all_quotas_usage);
}
}
