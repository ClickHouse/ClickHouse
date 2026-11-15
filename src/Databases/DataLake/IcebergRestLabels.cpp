#include <Databases/DataLake/IcebergRestLabels.h>

#include <Common/checkStackSize.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/Exception.h>
#include <Poco/JSON/Array.h>

#include <map>
#include <unordered_map>

namespace DataLake
{

namespace
{

using FieldNames = std::unordered_map<Int32, std::string>;

std::optional<Int32> tryGetInt32(const Poco::JSON::Object::Ptr & object, const std::string & key)
{
    if (!object || !object->has(key))
        return {};

    const auto value = object->get(key);
    if (!value.isInteger())
        return {};

    try
    {
        return value.convert<Int32>();
    }
    catch (const Poco::Exception &)
    {
        /// Does not fit into the type of a field id.
        return {};
    }
}

std::optional<std::string> tryGetString(const Poco::JSON::Object::Ptr & object, const std::string & key)
{
    if (!object || !object->has(key))
        return {};

    const auto value = object->get(key);
    if (!value.isString())
        return {};

    return value.extract<std::string>();
}

/// Labels are a flat map from string to string. A value of another scalar type is kept as its text,
/// while null and nested values, which have no sensible text, are skipped.
TableLabels::Labels parseLabels(const Poco::JSON::Object::Ptr & object)
{
    TableLabels::Labels result;
    if (!object)
        return result;

    for (const auto & [key, value] : *object)
    {
        if (value.isString())
            result[key] = value.extract<std::string>();
        else if (value.isNumeric() || value.isBoolean())
            result[key] = value.convert<std::string>();
    }
    return result;
}

void collectNestedFieldNames(const Poco::Dynamic::Var & type, const std::string & parent_name, FieldNames & names);

void addField(Int32 id, const std::string & name, const Poco::Dynamic::Var & type, FieldNames & names)
{
    names.emplace(id, name);
    collectNestedFieldNames(type, name, names);
}

/// Names the fields nested in `type` the way Iceberg does: a field of a struct is `parent.field`,
/// the element of a list is `parent.element`, the key and the value of a map are `parent.key` and `parent.value`.
void collectNestedFieldNames(const Poco::Dynamic::Var & type, const std::string & parent_name, FieldNames & names)
{
    checkStackSize();

    /// A primitive type is a string and has no nested fields.
    if (type.isEmpty() || type.type() != typeid(Poco::JSON::Object::Ptr))
        return;

    const auto object = type.extract<Poco::JSON::Object::Ptr>();
    const auto type_name = tryGetString(object, "type");
    if (!type_name)
        return;

    if (*type_name == "struct")
    {
        const auto fields = object->getArray("fields");
        if (!fields)
            return;

        for (UInt32 i = 0; i < fields->size(); ++i)
        {
            const auto field = fields->getObject(i);
            const auto id = tryGetInt32(field, "id");
            const auto name = tryGetString(field, "name");
            if (id && name)
                addField(*id, parent_name.empty() ? *name : parent_name + "." + *name, field->get("type"), names);
        }
    }
    else if (*type_name == "list")
    {
        if (const auto id = tryGetInt32(object, "element-id"))
            addField(*id, parent_name + ".element", object->get("element"), names);
    }
    else if (*type_name == "map")
    {
        if (const auto id = tryGetInt32(object, "key-id"))
            addField(*id, parent_name + ".key", object->get("key"), names);
        if (const auto id = tryGetInt32(object, "value-id"))
            addField(*id, parent_name + ".value", object->get("value"), names);
    }
}

Poco::JSON::Object::Ptr getCurrentSchema(const Poco::JSON::Object::Ptr & metadata)
{
    if (!metadata)
        return nullptr;

    if (const auto schemas = metadata->getArray("schemas"))
    {
        if (const auto current_schema_id = tryGetInt32(metadata, "current-schema-id"))
        {
            for (UInt32 i = 0; i < schemas->size(); ++i)
            {
                const auto schema = schemas->getObject(i);
                if (tryGetInt32(schema, "schema-id") == current_schema_id)
                    return schema;
            }
        }
    }

    /// A table of format version 1 may have only the `schema` field.
    return metadata->getObject("schema");
}

FieldNames getFieldNamesOfCurrentSchema(const Poco::JSON::Object::Ptr & metadata)
{
    FieldNames names;
    if (const auto schema = getCurrentSchema(metadata))
        collectNestedFieldNames(Poco::Dynamic::Var(schema), /* parent_name = */ "", names);
    return names;
}

}

TableLabels parseIcebergRestTableLabels(const Poco::JSON::Object::Ptr & load_table_result)
{
    TableLabels result;
    if (!load_table_result)
        return result;

    const auto labels = load_table_result->getObject("labels");
    if (!labels)
        return result;

    result.object_labels = parseLabels(labels->getObject("object-labels"));

    const auto fields = labels->getArray("fields");
    if (!fields || fields->size() == 0)
        return result;

    /// One entry per field is expected, but nothing forbids several: merge them.
    std::map<Int32, TableLabels::Labels> labels_by_field_id;
    for (UInt32 i = 0; i < fields->size(); ++i)
    {
        const auto entry = fields->getObject(i);
        const auto field_id = tryGetInt32(entry, "field-id");
        if (!field_id)
            continue;

        auto & field_labels = labels_by_field_id[*field_id];
        for (auto & [key, value] : parseLabels(entry->getObject("labels")))
            field_labels[key] = std::move(value);
    }

    const auto field_names = getFieldNamesOfCurrentSchema(load_table_result->getObject("metadata"));

    result.field_labels.reserve(labels_by_field_id.size());
    for (auto & [field_id, field_labels] : labels_by_field_id)
    {
        if (field_labels.empty())
            continue;

        TableLabels::FieldLabels field;
        field.field_id = field_id;
        if (const auto it = field_names.find(field_id); it != field_names.end())
            field.field_name = it->second;
        field.labels = std::move(field_labels);
        result.field_labels.push_back(std::move(field));
    }

    return result;
}

}
