#include <Interpreters/InterpreterCreateEndpointQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterMetadataManager.h>
#include <Common/Clusters/PropertyValidation.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateEndpointQuery.h>


namespace DB
{

using namespace SQLClusterCatalog;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool parseBoolProperty(const Field & value)
{
    if (value.getType() == Field::Types::Bool)
        return value.safeGet<bool>();
    return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value) != 0;
}

String parseStringProperty(const Field & value, std::string_view property_name)
{
    if (value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` in endpoint-level PROPERTIES must be a string", property_name);
    return value.safeGet<String>();
}

EndpointCatalogDefinition makeEndpointDefinition(const SettingsChanges & properties)
{
    PropertyValidation::Replica::validateForSQLReplica(properties);

    EndpointCatalogDefinition definition;
    for (const auto & change : properties)
    {
        if (change.name == "host")
            definition.host = parseStringProperty(change.value, change.name);
        else if (change.name == "port")
        {
            definition.port = PropertyValidation::Replica::narrowPortToUInt16(
                PropertyValidation::Detail::parseUnsignedIntegerPropertyValue(change.value, "port"),
                "Property `port`");
        }
        else if (change.name == "user")
            definition.user = parseStringProperty(change.value, change.name);
        else if (change.name == "password")
            definition.password = parseStringProperty(change.value, change.name);
        else if (change.name == "default_database")
            definition.default_database = parseStringProperty(change.value, change.name);
        else if (change.name == "bind_host")
            definition.bind_host = parseStringProperty(change.value, change.name);
        else if (change.name == "secure")
            definition.secure = parseBoolProperty(change.value);
        else if (change.name == "compression")
            definition.compression = parseBoolProperty(change.value);
        else if (change.name == "priority")
        {
            definition.priority = PropertyValidation::Replica::narrowPriorityToInt64(
                PropertyValidation::Detail::parseUnsignedIntegerPropertyValue(change.value, "priority"),
                "Property `priority`");
        }
    }
    return definition;
}

}

BlockIO InterpreterCreateEndpointQuery::execute()
{
    auto current_context = getContext();
    const auto & endpoint_query = query_ptr->as<const ASTCreateEndpointQuery &>();

    current_context->checkAccess(AccessType::CREATE_ENDPOINT);

    ClusterMetadataManager::instance().createEndpoint(
        endpoint_query.endpoint_name,
        makeEndpointDefinition(endpoint_query.properties),
        endpoint_query.if_not_exists);
    return {};
}

void registerInterpreterCreateEndpointQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateEndpointQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateEndpointQuery>(args.query, args.context); });
}

}
