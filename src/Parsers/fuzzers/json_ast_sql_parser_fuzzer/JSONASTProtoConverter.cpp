#include <Parsers/fuzzers/json_ast_sql_parser_fuzzer/JSONASTProtoConverter.h>

#include <Common/Exception.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/Exception.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <google/protobuf/descriptor.h>

#include <charconv>
#include <cmath>
#include <unordered_map>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::JSONASTFuzzer
{

namespace
{

using namespace json_ast_fuzzer;

/// The `(json_text)` option of an enum value, or an empty view when the value is unknown.
template <typename Enum>
std::string_view enumJSONText(const google::protobuf::EnumDescriptor * descriptor, Enum value)
{
    const auto * value_descriptor = descriptor->FindValueByNumber(static_cast<int>(value));
    if (!value_descriptor)
        return {};
    return value_descriptor->options().GetExtension(json_text);
}

/// Reverse lookup table `json text -> enum value`, built once from the descriptor.
template <typename Enum>
const std::unordered_map<std::string_view, Enum> & textToEnumTable(const google::protobuf::EnumDescriptor * descriptor)
{
    static const std::unordered_map<std::string_view, Enum> table = [descriptor]
    {
        std::unordered_map<std::string_view, Enum> result;
        for (int i = 0; i < descriptor->value_count(); ++i)
        {
            const auto * value_descriptor = descriptor->value(i);
            const std::string & text = value_descriptor->options().GetExtension(json_text);
            if (!text.empty())
                result.emplace(text, static_cast<Enum>(value_descriptor->number()));
        }
        return result;
    }();
    return table;
}

/// ---------------------------------------------------------------------------------------------
/// Protobuf -> JSON text
/// ---------------------------------------------------------------------------------------------

class Writer
{
public:
    Writer(const ProtoToJSONLimits & limits_, std::string & out_) : limits(limits_), out(out_) {}

    bool writeRoot(const Node & node)
    {
        writeNode(node, 0);
        return ok;
    }

private:
    const ProtoToJSONLimits & limits;
    std::string & out;
    bool ok = true;

    bool enter(size_t depth)
    {
        if (!ok)
            return false;
        if (depth > limits.max_depth || out.size() > limits.max_output_bytes)
        {
            ok = false;
            return false;
        }
        return true;
    }

    void writeEscapedString(std::string_view s)
    {
        out.push_back('"');
        for (unsigned char c : s)
        {
            switch (c)
            {
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                case '\b': out += "\\b"; break;
                case '\f': out += "\\f"; break;
                default:
                    if (c < 0x20)
                    {
                        static constexpr char hex[] = "0123456789abcdef";
                        out += "\\u00";
                        out.push_back(hex[c >> 4]);
                        out.push_back(hex[c & 0xF]);
                    }
                    else
                        out.push_back(static_cast<char>(c));
            }
        }
        out.push_back('"');
    }

    void writeDouble(double value)
    {
        /// Non-finite values are not representable in JSON; the `Field` reader accepts these
        /// string sentinels for `Float64` and `Null` (see `JSONObjectReader::readFieldFromObject`).
        if (std::isnan(value))
        {
            out += "\"nan\"";
            return;
        }
        if (std::isinf(value))
        {
            out += value > 0 ? "\"+Inf\"" : "\"-Inf\"";
            return;
        }
        char buf[64];
        auto res = std::to_chars(buf, buf + sizeof(buf), value);
        out.append(buf, res.ptr);
    }

    void writeKeyPrefix(std::string_view key, bool & first)
    {
        if (!first)
            out.push_back(',');
        first = false;
        writeEscapedString(key);
        out.push_back(':');
    }

    void writeString(const StringItem & item)
    {
        switch (item.value_case())
        {
            case StringItem::kRaw:
                writeEscapedString(item.raw());
                break;
            case StringItem::kKnown:
                writeEscapedString(knownStringText(item.known()));
                break;
            case StringItem::VALUE_NOT_SET:
                out += "\"\"";
                break;
        }
    }

    void writeField(const FieldValue & field, size_t depth)
    {
        if (!enter(depth))
            return;
        out.push_back('{');
        bool first = true;
        if (field.field_type() != F_UNSET)
        {
            writeKeyPrefix("field_type", first);
            writeEscapedString(fieldTypeText(field.field_type()));
        }
        if (field.value_case() != FieldValue::VALUE_NOT_SET)
        {
            writeKeyPrefix("value", first);
            switch (field.value_case())
            {
                case FieldValue::kNullValue: out += "null"; break;
                case FieldValue::kBoolValue: out += field.bool_value() ? "true" : "false"; break;
                case FieldValue::kIntValue: out += std::to_string(field.int_value()); break;
                case FieldValue::kUintValue: out += std::to_string(field.uint_value()); break;
                case FieldValue::kDoubleValue: writeDouble(field.double_value()); break;
                case FieldValue::kStringValue: writeEscapedString(field.string_value()); break;
                case FieldValue::kElements:
                {
                    out.push_back('[');
                    bool first_element = true;
                    for (const auto & element : field.elements().items())
                    {
                        if (!first_element)
                            out.push_back(',');
                        first_element = false;
                        writeField(element, depth + 1);
                        if (!ok)
                            return;
                    }
                    out.push_back(']');
                    break;
                }
                case FieldValue::VALUE_NOT_SET:
                    break;
            }
        }
        out.push_back('}');
    }

    /// Writes the properties of a node or of a plain object. Duplicate keys are dropped (the first
    /// occurrence wins) so that the rendered document stays a well-formed JSON object; `reserved`
    /// holds the keys the caller emits itself (`type`, `children`).
    void writeProperties(const google::protobuf::RepeatedPtrField<Property> & props, std::vector<std::string_view> reserved, size_t depth, bool & first)
    {
        for (const auto & prop : props)
        {
            if (!ok)
                return;
            if (prop.key() == K_UNSET || prop.value_case() == Property::VALUE_NOT_SET)
                continue;
            std::string_view key = keyText(prop.key());
            if (key.empty())
                continue;
            bool duplicate = false;
            for (const auto & seen : reserved)
                if (seen == key)
                    duplicate = true;
            if (duplicate)
                continue;
            reserved.push_back(key);

            writeKeyPrefix(key, first);
            switch (prop.value_case())
            {
                case Property::kBoolValue: out += prop.bool_value() ? "true" : "false"; break;
                case Property::kIntValue: out += std::to_string(prop.int_value()); break;
                case Property::kUintValue: out += std::to_string(prop.uint_value()); break;
                case Property::kDoubleValue: writeDouble(prop.double_value()); break;
                case Property::kStringValue: writeEscapedString(prop.string_value()); break;
                case Property::kKnownString: writeEscapedString(knownStringText(prop.known_string())); break;
                case Property::kNodeValue: writeNode(prop.node_value(), depth + 1); break;
                case Property::kNodeList:
                {
                    out.push_back('[');
                    bool first_item = true;
                    for (const auto & item : prop.node_list().items())
                    {
                        if (!first_item)
                            out.push_back(',');
                        first_item = false;
                        writeNode(item, depth + 1);
                        if (!ok)
                            return;
                    }
                    out.push_back(']');
                    break;
                }
                case Property::kFieldValue: writeField(prop.field_value(), depth + 1); break;
                case Property::kStringList:
                {
                    out.push_back('[');
                    bool first_item = true;
                    for (const auto & item : prop.string_list().items())
                    {
                        if (!first_item)
                            out.push_back(',');
                        first_item = false;
                        writeString(item);
                    }
                    out.push_back(']');
                    break;
                }
                case Property::kObjectValue: writeObject(prop.object_value(), depth + 1); break;
                case Property::kObjectList:
                {
                    out.push_back('[');
                    bool first_item = true;
                    for (const auto & item : prop.object_list().items())
                    {
                        if (!first_item)
                            out.push_back(',');
                        first_item = false;
                        writeObject(item, depth + 1);
                        if (!ok)
                            return;
                    }
                    out.push_back(']');
                    break;
                }
                case Property::kNullValue: out += "null"; break;
                case Property::kFunctionName: writeEscapedString(enumJSONText(FunctionName_descriptor(), prop.function_name())); break;
                case Property::kSettingName: writeEscapedString(enumJSONText(SettingName_descriptor(), prop.setting_name())); break;
                case Property::VALUE_NOT_SET: break;
            }
        }
    }

    void writeObject(const Object & object, size_t depth)
    {
        if (!enter(depth))
            return;
        out.push_back('{');
        bool first = true;
        writeProperties(object.props(), {}, depth, first);
        out.push_back('}');
    }

    void writeNode(const Node & node, size_t depth)
    {
        if (!enter(depth))
            return;
        out.push_back('{');
        bool first = true;
        if (node.type() != T_UNSET)
        {
            writeKeyPrefix("type", first);
            writeEscapedString(nodeTypeText(node.type()));
        }
        writeProperties(node.props(), {"type", "children"}, depth, first);
        if (!ok)
            return;
        if (node.children_size() > 0)
        {
            writeKeyPrefix("children", first);
            out.push_back('[');
            bool first_child = true;
            for (const auto & child : node.children())
            {
                if (!first_child)
                    out.push_back(',');
                first_child = false;
                writeNode(child, depth + 1);
                if (!ok)
                    return;
            }
            out.push_back(']');
        }
        out.push_back('}');
    }
};

/// ---------------------------------------------------------------------------------------------
/// JSON text -> protobuf
/// ---------------------------------------------------------------------------------------------

[[noreturn]] void throwBadArguments(const std::string & message)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", message);
}

bool isObject(const Poco::Dynamic::Var & var)
{
    return var.type() == typeid(Poco::JSON::Object::Ptr);
}

bool isArray(const Poco::Dynamic::Var & var)
{
    return var.type() == typeid(Poco::JSON::Array::Ptr);
}

Poco::JSON::Object::Ptr asObject(const Poco::Dynamic::Var & var, const std::string & what)
{
    if (!isObject(var))
        throwBadArguments("Expected a JSON object for " + what);
    auto ptr = var.extract<Poco::JSON::Object::Ptr>();
    if (!ptr)
        throwBadArguments("Expected a non-null JSON object for " + what);
    return ptr;
}

/// Stores a JSON integer into the signed/unsigned pair of a `oneof`.
template <typename Message>
void setInteger(const Poco::Dynamic::Var & var, Message & message)
{
    if (var.isSigned())
    {
        Int64 value = var.convert<Int64>();
        if (value < 0)
            message.set_int_value(value);
        else
            message.set_uint_value(static_cast<UInt64>(value));
    }
    else
        message.set_uint_value(var.convert<UInt64>());
}

void objectToNode(const Poco::JSON::Object & object, Node & node);

/// An AST node is an object whose `type` is a registered node type name. Plain objects may have a
/// `type` property too (`server_type.type`, the `type` of a `BACKUP` element), so the key alone
/// does not decide.
bool isNodeObject(const Poco::JSON::Object & object)
{
    if (!object.has("type") || !object.get("type").isString())
        return false;
    return textToEnumTable<NodeType>(NodeType_descriptor()).contains(object.getValue<std::string>("type"));
}

void objectToField(const Poco::JSON::Object & object, FieldValue & field)
{
    if (!object.has("field_type") || !object.get("field_type").isString())
        throwBadArguments("Field value without a string 'field_type'");
    const auto & field_types = textToEnumTable<FieldType>(FieldType_descriptor());
    std::string type_text = object.getValue<std::string>("field_type");
    auto it = field_types.find(type_text);
    if (it == field_types.end())
        throwBadArguments("Unknown field_type '" + type_text + "', add it to json_ast.proto");
    field.set_field_type(it->second);

    for (const auto & [key, value] : object)
    {
        if (key == "field_type")
            continue;
        if (key != "value")
            throwBadArguments("Unexpected key '" + key + "' in a Field value");

        if (value.isEmpty())
            field.set_null_value(true);
        else if (value.isBoolean())
            field.set_bool_value(value.convert<bool>());
        else if (value.isInteger())
            setInteger(value, field);
        else if (value.isNumeric())
            field.set_double_value(value.convert<double>());
        else if (value.isString())
            field.set_string_value(value.convert<std::string>());
        else if (isArray(value))
        {
            auto array = value.extract<Poco::JSON::Array::Ptr>();
            auto * elements = field.mutable_elements();
            for (unsigned int i = 0; array && i < array->size(); ++i)
                objectToField(*asObject(array->get(i), "an element of a structured Field value"), *elements->add_items());
        }
        else
            throwBadArguments("Unsupported JSON value kind for a Field value");
    }
}

void varToProperty(const std::string & key, const Poco::Dynamic::Var & value, Property & prop)
{
    Key key_enum;
    if (!Key_Parse("K_" + key, &key_enum) || key_enum == K_UNSET)
        throwBadArguments("Unknown JSON AST property key '" + key + "', add it to json_ast.proto");
    prop.set_key(key_enum);

    const auto & known_strings = textToEnumTable<KnownString>(KnownString_descriptor());

    if (value.isEmpty())
        prop.set_null_value(true);
    else if (value.isBoolean())
        prop.set_bool_value(value.convert<bool>());
    else if (value.isInteger())
        setInteger(value, prop);
    else if (value.isNumeric())
        prop.set_double_value(value.convert<double>());
    else if (value.isString())
    {
        std::string text = value.convert<std::string>();
        const auto & function_names = textToEnumTable<FunctionName>(FunctionName_descriptor());
        const auto & setting_names = textToEnumTable<SettingName>(SettingName_descriptor());
        if (auto it = known_strings.find(text); it != known_strings.end())
            prop.set_known_string(it->second);
        else if (auto fn = function_names.find(text); key == "name" && fn != function_names.end())
            prop.set_function_name(fn->second);
        else if (auto st = setting_names.find(text); key == "name" && st != setting_names.end())
            prop.set_setting_name(st->second);
        else
            prop.set_string_value(text);
    }
    else if (isObject(value))
    {
        auto object = asObject(value, "property '" + key + "'");
        if (object->has("field_type"))
            objectToField(*object, *prop.mutable_field_value());
        else if (isNodeObject(*object))
            objectToNode(*object, *prop.mutable_node_value());
        else
        {
            auto * plain = prop.mutable_object_value();
            for (const auto & [nested_key, nested_value] : *object)
                varToProperty(nested_key, nested_value, *plain->add_props());
        }
    }
    else if (isArray(value))
    {
        auto array = value.extract<Poco::JSON::Array::Ptr>();
        const size_t size = array ? array->size() : 0;

        bool all_strings = true;
        bool all_objects = true;
        bool all_nodes = true;
        for (unsigned int i = 0; i < size; ++i)
        {
            const auto element = array->get(i);
            all_strings = all_strings && element.isString();
            bool object = isObject(element);
            all_objects = all_objects && object;
            all_nodes = all_nodes && object && isNodeObject(*element.extract<Poco::JSON::Object::Ptr>());
        }

        if (all_strings)
        {
            /// Also the representation of an empty array.
            auto * list = prop.mutable_string_list();
            for (unsigned int i = 0; i < size; ++i)
            {
                std::string text = array->get(i).convert<std::string>();
                auto * item = list->add_items();
                if (auto it = known_strings.find(text); it != known_strings.end())
                    item->set_known(it->second);
                else
                    item->set_raw(text);
            }
        }
        else if (all_nodes)
        {
            auto * list = prop.mutable_node_list();
            for (unsigned int i = 0; i < size; ++i)
                objectToNode(*array->get(i).extract<Poco::JSON::Object::Ptr>(), *list->add_items());
        }
        else if (all_objects)
        {
            auto * list = prop.mutable_object_list();
            for (unsigned int i = 0; i < size; ++i)
            {
                auto * plain = list->add_items();
                for (const auto & [nested_key, nested_value] : *array->get(i).extract<Poco::JSON::Object::Ptr>())
                    varToProperty(nested_key, nested_value, *plain->add_props());
            }
        }
        else
            throwBadArguments("Array property '" + key + "' mixes value kinds, which json_ast.proto cannot express");
    }
    else
        throwBadArguments("Unsupported JSON value kind for property '" + key + "'");
}

void objectToNode(const Poco::JSON::Object & object, Node & node)
{
    if (!object.has("type") || !object.get("type").isString())
        throwBadArguments("AST node object without a string 'type'");
    const auto & node_types = textToEnumTable<NodeType>(NodeType_descriptor());
    std::string type_text = object.getValue<std::string>("type");
    auto it = node_types.find(type_text);
    if (it == node_types.end())
        throwBadArguments("Unknown AST node type '" + type_text + "', add it to json_ast.proto");
    node.set_type(it->second);

    for (const auto & [key, value] : object)
    {
        if (key == "type")
            continue;
        if (key == "children")
        {
            if (!isArray(value))
                throwBadArguments("'children' is not a JSON array");
            auto array = value.extract<Poco::JSON::Array::Ptr>();
            for (unsigned int i = 0; array && i < array->size(); ++i)
                objectToNode(*asObject(array->get(i), "an element of 'children'"), *node.add_children());
            continue;
        }
        varToProperty(key, value, *node.add_props());
    }
}

}

bool protoToJSON(const Node & node, const ProtoToJSONLimits & limits, std::string & out)
{
    Writer writer(limits, out);
    return writer.writeRoot(node);
}

void jsonToProto(const std::string & json, Node & out)
{
    Poco::Dynamic::Var parsed;
    try
    {
        Poco::JSON::Parser parser;
        parsed = parser.parse(json);
    }
    catch (const Poco::Exception & e)
    {
        throwBadArguments("Failed to parse JSON: " + e.displayText());
    }
    out.Clear();
    objectToNode(*asObject(parsed, "the root of the JSON AST"), out);
}

std::string_view nodeTypeText(NodeType type)
{
    return enumJSONText(NodeType_descriptor(), type);
}

std::string_view fieldTypeText(FieldType type)
{
    return enumJSONText(FieldType_descriptor(), type);
}

std::string_view keyText(Key key)
{
    const auto * value_descriptor = Key_descriptor()->FindValueByNumber(static_cast<int>(key));
    if (!value_descriptor)
        return {};
    std::string_view name = value_descriptor->name();
    /// `K_alias` -> `alias`
    return name.size() > 2 ? name.substr(2) : std::string_view{};
}

std::string_view knownStringText(KnownString value)
{
    return enumJSONText(KnownString_descriptor(), value);
}

}
