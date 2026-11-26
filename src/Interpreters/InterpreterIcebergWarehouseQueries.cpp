#include <Interpreters/InterpreterIcebergWarehouseQueries.h>

#include <Access/Common/AccessFlags.h>
#include <Common/quoteString.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateIcebergWarehouseQuery.h>
#include <Parsers/ASTDropIcebergWarehouseQuery.h>
#include <QueryPipeline/BlockIO.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace ServerSetting
{
    extern const ServerSettingsBool enable_iceberg_rest_catalog;
}

namespace
{

/// Registry for one statement. Catalog server reads Keeper on each request, so it sees the change.
IcebergRESTCatalogWarehouseRegistry createRegistry(const ContextPtr & context)
{
    if (!context->getServerSettings()[ServerSetting::enable_iceberg_rest_catalog])
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "The Iceberg REST catalog is experimental. Enable the server setting `enable_iceberg_rest_catalog` to use it");

    return IcebergRESTCatalogWarehouseRegistry(context->getGlobalContext());
}

String getStringValue(const SettingChange & change)
{
    if (change.value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `{}` of an Iceberg warehouse must be a string", change.name);
    return change.value.safeGet<String>();
}

/// Each setting is one field of the definition.
IcebergRESTCatalogWarehouseDefinition definitionFromSettings(const SettingsChanges & changes)
{
    IcebergRESTCatalogWarehouseDefinition definition;
    for (const auto & change : changes)
    {
        if (change.name == "base_location")
            definition.base_location = getStringValue(change);
        else if (change.name == "role_arn")
            definition.credential.role_arn = getStringValue(change);
        else if (change.name == "external_id")
            definition.credential.external_id = getStringValue(change);
        else if (change.name == "role_session_name")
            definition.credential.role_session_name = getStringValue(change);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting `{}` of an Iceberg warehouse", change.name);
    }

    definition.validate();
    return definition;
}

}

BlockIO InterpreterCreateIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::CREATE_ICEBERG_WAREHOUSE);

    IcebergRESTCatalogWarehouseDefinition::validateName(query.warehouse_name);
    auto definition = definitionFromSettings(query.changes);

    auto registry = createRegistry(getContext());
    if (!registry.create(query.warehouse_name, std::move(definition)) && !query.if_not_exists)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} already exists", backQuote(query.warehouse_name));

    return {};
}

BlockIO InterpreterDropIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::DROP_ICEBERG_WAREHOUSE);

    using Result = IcebergRESTCatalogWarehouseRegistry::DropResult;
    auto registry = createRegistry(getContext());
    switch (registry.drop(query.warehouse_name))
    {
        case Result::Dropped:
            return {};
        case Result::NotFound:
            if (query.if_exists)
                return {};
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} does not exist", backQuote(query.warehouse_name));
        case Result::NotEmpty:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} is not empty. Drop its namespaces first", backQuote(query.warehouse_name));
    }
    UNREACHABLE();
}

void registerInterpreterIcebergWarehouseQueries(InterpreterFactory & factory);
void registerInterpreterIcebergWarehouseQueries(InterpreterFactory & factory)
{
    factory.registerInterpreter("InterpreterCreateIcebergWarehouseQuery", [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateIcebergWarehouseQuery>(args.query, args.context);
    });
    factory.registerInterpreter("InterpreterDropIcebergWarehouseQuery", [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropIcebergWarehouseQuery>(args.query, args.context);
    });
}

}
