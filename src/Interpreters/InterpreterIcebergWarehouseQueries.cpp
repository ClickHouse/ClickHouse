#include <Interpreters/InterpreterIcebergWarehouseQueries.h>

#include <Access/Common/AccessFlags.h>
#include <Common/quoteString.h>
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
}

namespace
{

constexpr auto BASE_LOCATION = "base_location";
constexpr auto ROLE_ARN = "role_arn";
constexpr auto EXTERNAL_ID = "external_id";
constexpr auto ROLE_SESSION_NAME = "role_session_name";

bool isCredentialSetting(std::string_view name)
{
    return name == ROLE_ARN || name == EXTERNAL_ID || name == ROLE_SESSION_NAME;
}

String getStringValue(const SettingChange & change)
{
    if (change.value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `{}` of an Iceberg warehouse must be a string", change.name);
    return change.value.safeGet<String>();
}

/// The settings are the fields of the management API request with the `credential` object flattened.
/// Building that request keeps one validation path for SQL and REST.
IcebergRESTCatalogWarehouseDefinition definitionFromSettings(const SettingsChanges & changes)
{
    Poco::JSON::Object::Ptr credential = new Poco::JSON::Object;
    /// `role_arn` implies the credential type.
    credential->set("type", IcebergRESTCatalogWarehouseCredential::TYPE_AWS_ROLE);

    Poco::JSON::Object json;
    for (const auto & change : changes)
    {
        if (change.name == BASE_LOCATION)
            json.set(change.name, getStringValue(change));
        else if (isCredentialSetting(change.name))
            credential->set(change.name, getStringValue(change));
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting `{}` of an Iceberg warehouse", change.name);
    }
    json.set("credential", credential);

    return IcebergRESTCatalogWarehouseDefinition::fromJSON(json);
}

}

BlockIO InterpreterCreateIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::CREATE_ICEBERG_WAREHOUSE);

    IcebergRESTCatalogWarehouseDefinition::validateName(query.warehouse_name);
    auto definition = definitionFromSettings(query.changes);

    const auto registry = getContext()->getIcebergRESTCatalogWarehouseRegistry();
    if (!registry->create(query.warehouse_name, std::move(definition)) && !query.if_not_exists)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} already exists", backQuote(query.warehouse_name));

    return {};
}

BlockIO InterpreterDropIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::DROP_ICEBERG_WAREHOUSE);

    using Result = IcebergRESTCatalogWarehouseRegistry::DropResult;
    switch (getContext()->getIcebergRESTCatalogWarehouseRegistry()->drop(query.warehouse_name))
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
