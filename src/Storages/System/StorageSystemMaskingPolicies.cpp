#include <Storages/System/StorageSystemMaskingPolicies.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemMaskingPolicies::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of a masking policy."},
        {"short_name", std::make_shared<DataTypeString>(),
            "Short name of a masking policy. Names of masking policies are compound, for example: mask_email ON mydb.mytable. "
            "Here 'mask_email ON mydb.mytable' is the name of the masking policy, and 'mask_email' is its short name."},
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"id", std::make_shared<DataTypeUUID>(), "Masking policy ID."},
        {"storage", std::make_shared<DataTypeString>(), "Name of the directory where the masking policy is stored."},
        {"update_assignments", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "UPDATE assignments that define how data should be masked, for example: email = '***masked***', phone = '***-***-****'. NULL if not set."},
        {"where_condition", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "Optional condition selecting the rows the mask is applied to. NULL if not set."},
        {"priority", std::make_shared<DataTypeInt64>(),
            "Priority of the masking policy. Policies with higher priority are applied first."},
        {"apply_to_all", std::make_shared<DataTypeUInt8>(),
            "Shows that the masking policy is set for all roles and/or users."},
        {"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "List of the roles and/or users to which the masking policy is applied."},
        {"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "The masking policy is applied to all roles and/or users except the listed ones."},
    };
}


void StorageSystemMaskingPolicies::fillData(MutableColumns &, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_MASKING_POLICIES);

    /// Masking policies are implemented only in ClickHouse Cloud, so in open-source builds there are never
    /// any entities to list and the table is always empty. The table and the access grant still exist so
    /// that introspection queries such as `SHOW MASKING POLICIES` work and do not throw.
}

}
