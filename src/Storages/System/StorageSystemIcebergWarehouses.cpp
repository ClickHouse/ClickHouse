#include <Storages/System/StorageSystemIcebergWarehouses.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>


namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsBool enable_iceberg_rest_catalog;
}

ColumnsDescription StorageSystemIcebergWarehouses::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the warehouse."},
        {"uuid", std::make_shared<DataTypeUUID>(), "UUID of the warehouse. A warehouse dropped and created again under the same name gets a new one."},
        {"base_location", std::make_shared<DataTypeString>(), "Default storage prefix for tables of the warehouse."},
        {"role_arn", std::make_shared<DataTypeString>(), "IAM role that the server assumes to access the warehouse storage."},
    };
}

StorageSystemIcebergWarehouses::StorageSystemIcebergWarehouses(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemIcebergWarehouses::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    if (!context->getAccess()->isGranted(AccessType::SHOW_ICEBERG_WAREHOUSES))
        return;

    /// A server without the catalog has no warehouses.
    if (!context->getServerSettings()[ServerSetting::enable_iceberg_rest_catalog])
        return;

    for (const auto & [name, definition] : context->getIcebergRESTCatalogWarehouseRegistry()->list())
    {
        size_t col = 0;
        res_columns[col++]->insert(name);
        res_columns[col++]->insert(definition.uuid);
        res_columns[col++]->insert(definition.base_location);
        res_columns[col++]->insert(definition.credential.role_arn);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemIcebergWarehouses) }
