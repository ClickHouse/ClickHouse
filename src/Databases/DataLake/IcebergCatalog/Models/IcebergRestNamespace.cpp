#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestNamespace.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <base/find_symbols.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <sstream>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DataLake::IcebergRestModels
{

std::string encodeNamespaceForURI(const std::string & namespace_name)
{
    std::string encoded;
    encoded.reserve(namespace_name.size());
    for (const auto ch : namespace_name)
    {
        if (ch == '.')
            encoded += "%1F";
        else
            encoded.push_back(ch);
    }
    return encoded;
}

Poco::URI::QueryParameters createParentNamespaceQueryParams(const std::string & base_namespace)
{
    std::vector<std::string_view> parts;
    splitInto<'.'>(parts, base_namespace);
    std::string parent_param;
    for (const auto & part : parts)
    {
        if (!parent_param.empty())
            parent_param += static_cast<char>(0x1F);
        parent_param += part;
    }
    return {{"parent", parent_param}};
}

NamespaceListPage parseNamespaceListPage(
    const std::string & json,
    const std::string & base_namespace,
    const NamespaceListParseOptions & options)
{
    NamespaceListPage result;

    if (json.empty())
        return result;

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed = parser.parse(json);
    if (parsed.type() == typeid(Poco::JSON::Object::Ptr))
    {
        const auto & obj = parsed.extract<Poco::JSON::Object::Ptr>();
        if (obj->size() == 0)
            return result;
    }

    const auto & object = parsed.extract<Poco::JSON::Object::Ptr>();
    auto namespaces_object = object->get("namespaces").extract<Poco::JSON::Array::Ptr>();
    if (!namespaces_object)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse namespace list result");

    size_t skipped_entries = 0;
    for (size_t i = 0; i < namespaces_object->size(); ++i)
    {
        auto current_namespace_array = namespaces_object->get(static_cast<int>(i)).extract<Poco::JSON::Array::Ptr>();
        if (current_namespace_array->size() == 0)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expected namespace array to be non-empty");

        const int idx = static_cast<int>(current_namespace_array->size()) - 1;
        const auto current_namespace = current_namespace_array->get(idx).extract<std::string>();

        if (options.skip_subnamespaces_when_parent_non_empty && !base_namespace.empty())
        {
            ++skipped_entries;
            continue;
        }

        const auto full_namespace = base_namespace.empty()
            ? current_namespace
            : base_namespace + "." + current_namespace;

        result.namespaces.push_back(full_namespace);
    }

    const bool flat_namespace_drops_all_entries = options.suppress_pagination_when_all_entries_skipped
        && options.skip_subnamespaces_when_parent_non_empty
        && !base_namespace.empty()
        && skipped_entries > 0
        && result.namespaces.empty();

    if (!flat_namespace_drops_all_entries
        && object->has("next-page-token")
        && !object->isNull("next-page-token"))
    {
        result.next_page_token = object->get("next-page-token").extract<std::string>();
    }

    return result;
}

Poco::JSON::Object::Ptr buildCreateNamespaceRequest(const std::string & namespace_name, const std::string & location)
{
    Poco::JSON::Object::Ptr request_body = new Poco::JSON::Object;

    Poco::JSON::Array::Ptr namespaces = new Poco::JSON::Array;
    namespaces->add(namespace_name);
    request_body->set("namespace", namespaces);

    Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
    properties->set("location", location);
    request_body->set("properties", properties);

    return request_body;
}

std::string serializeCreateNamespaceRequest(const std::string & namespace_name, const std::string & location)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    buildCreateNamespaceRequest(namespace_name, location)->stringify(oss);
    return oss.str();
}

std::string serializeNamespaceListPage(const NamespaceListPage & page)
{
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    Poco::JSON::Array::Ptr namespaces = new Poco::JSON::Array;

    for (const auto & full_namespace : page.namespaces)
    {
        Poco::JSON::Array::Ptr current = new Poco::JSON::Array;
        current->add(full_namespace);
        namespaces->add(current);
    }

    root->set("namespaces", namespaces);
    if (!page.next_page_token.empty())
        root->set("next-page-token", page.next_page_token);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(root, oss);
    return oss.str();
}

}

#endif
