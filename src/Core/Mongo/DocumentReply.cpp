#include <Core/Mongo/DocumentReply.h>

#include <unordered_map>

#include <DataTypes/DataTypeFactory.h>
#include <Parsers/Mongo/DocumentCollection.h>

namespace DB::MongoProtocol
{

namespace
{

const String & documentAlias()
{
    static const String alias(Mongo::RETURNED_DOCUMENT_ALIAS);
    return alias;
}

const String & typesAlias()
{
    static const String alias(Mongo::RETURNED_TYPES_ALIAS);
    return alias;
}

const String & objectIdColumn()
{
    static const String name(Mongo::OBJECT_ID_COLUMN);
    return name;
}

/// The type of every path of the document, as `JSONAllPathsWithTypes` reported it. A path that is
/// absent from it is a document of its own, whose members carry the types.
std::unordered_map<String, DataTypePtr> extractPathTypes(const rapidjson::Value & row)
{
    std::unordered_map<String, DataTypePtr> types;
    auto it = row.FindMember(typesAlias().c_str());
    if (it == row.MemberEnd() || !it->value.IsObject())
        return types;

    for (auto path = it->value.MemberBegin(); path != it->value.MemberEnd(); ++path)
    {
        if (!path->value.IsString())
            continue;
        types.emplace(path->name.GetString(), DataTypeFactory::instance().get(path->value.GetString()));
    }
    return types;
}

/// The type a value with no type of its own is converted by: a `Dynamic` carries no more
/// information than the JSON itself, so `appendTypedValue` converts such a value structurally.
DataTypePtr untypedValueType()
{
    static const DataTypePtr type = DataTypeFactory::instance().get("Dynamic");
    return type;
}

void appendMembers(
    bson_t * document, const rapidjson::Value & value, const String & prefix, const std::unordered_map<String, DataTypePtr> & types)
{
    for (auto member = value.MemberBegin(); member != value.MemberEnd(); ++member)
    {
        String name = member->name.GetString();
        String path = prefix.empty() ? name : prefix + "." + name;

        if (auto type = types.find(path); type != types.end())
        {
            appendTypedValue(document, name, member->value, type->second);
            continue;
        }

        /// A path that has no type of its own is an embedded document, which is returned as one.
        if (member->value.IsObject())
        {
            bson_t child;
            bson_append_document_begin(document, name.data(), static_cast<int>(name.size()), &child);
            appendMembers(&child, member->value, path, types);
            bson_append_document_end(document, &child);
            continue;
        }

        appendTypedValue(document, name, member->value, untypedValueType());
    }
}

}

void appendDocumentOfRow(bson_t * document, const rapidjson::Value & row)
{
    /// Mongo writes the object id as the first field of a document, and a driver reads it back as
    /// the identity of the document it inserted.
    if (auto it = row.FindMember(objectIdColumn().c_str()); it != row.MemberEnd() && it->value.IsString())
        BSON_APPEND_UTF8(document, objectIdColumn().c_str(), it->value.GetString());

    auto it = row.FindMember(documentAlias().c_str());
    if (it == row.MemberEnd() || !it->value.IsObject())
        return;

    appendMembers(document, it->value, "", extractPathTypes(row));
}

}
