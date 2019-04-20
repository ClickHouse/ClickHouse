#include <Common/config.h>
#if USE_CAPNP

#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Formats/CapnProtoRowInputStream.h> // Y_IGNORE
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <Formats/FormatSchemaInfo.h>
#include <capnp/serialize.h> // Y_IGNORE
#include <capnp/dynamic.h> // Y_IGNORE
#include <capnp/common.h> // Y_IGNORE
#include <boost/algorithm/string.hpp>
#include <boost/range/join.hpp>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int BAD_ARGUMENTS;
    extern const int THERE_IS_NO_COLUMN;
    extern const int LOGICAL_ERROR;
}

CapnProtoRowInputStream::NestedField split(const Block & header, size_t i)
{
    CapnProtoRowInputStream::NestedField field = {{}, i};

    // Remove leading dot in field definition, e.g. ".msg" -> "msg"
    String name(header.safeGetByPosition(i).name);
    if (name.size() > 0 && name[0] == '.')
        name.erase(0, 1);

    boost::split(field.tokens, name, boost::is_any_of("._"));
    return field;
}


Field convertNodeToField(capnp::DynamicValue::Reader value)
{
    switch (value.getType())
    {
        case capnp::DynamicValue::UNKNOWN:
            throw Exception("Unknown field type", ErrorCodes::BAD_TYPE_OF_FIELD);
        case capnp::DynamicValue::VOID:
            return Field();
        case capnp::DynamicValue::BOOL:
            return value.as<bool>() ? 1u : 0u;
        case capnp::DynamicValue::INT:
            return value.as<int64_t>();
        case capnp::DynamicValue::UINT:
            return value.as<uint64_t>();
        case capnp::DynamicValue::FLOAT:
            return value.as<double>();
        case capnp::DynamicValue::TEXT:
        {
            auto arr = value.as<capnp::Text>();
            return String(arr.begin(), arr.size());
        }
        case capnp::DynamicValue::DATA:
        {
            auto arr = value.as<capnp::Data>().asChars();
            return String(arr.begin(), arr.size());
        }
        case capnp::DynamicValue::LIST:
        {
            auto listValue = value.as<capnp::DynamicList>();
            Array res(listValue.size());
            for (auto i : kj::indices(listValue))
                res[i] = convertNodeToField(listValue[i]);

            return res;
        }
        case capnp::DynamicValue::ENUM:
            return value.as<capnp::DynamicEnum>().getRaw();
        case capnp::DynamicValue::STRUCT:
        {
            auto structValue = value.as<capnp::DynamicStruct>();
            const auto & fields = structValue.getSchema().getFields();

            Field field = Tuple(TupleBackend(fields.size()));
            TupleBackend & tuple = get<Tuple &>(field).toUnderType();
            for (auto i : kj::indices(fields))
                tuple[i] = convertNodeToField(structValue.get(fields[i]));

            return field;
        }
        case capnp::DynamicValue::CAPABILITY:
            throw Exception("CAPABILITY type not supported", ErrorCodes::BAD_TYPE_OF_FIELD);
        case capnp::DynamicValue::ANY_POINTER:
            throw Exception("ANY_POINTER type not supported", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
    return Field();
}

capnp::StructSchema::Field getFieldOrThrow(capnp::StructSchema node, const std::string & field)
{
    KJ_IF_MAYBE(child, node.findFieldByName(field))
        return *child;
    else
        throw Exception("Field " + field + " doesn't exist in schema " + node.getShortDisplayName().cStr(), ErrorCodes::THERE_IS_NO_COLUMN);
}


void CapnProtoRowInputStream::createActions(const NestedFieldList & sorted_fields, capnp::StructSchema reader)
{
    /// Columns in a table can map to fields in Cap'n'Proto or to structs.

    /// Store common parents and their tokens in order to backtrack.
    std::vector<capnp::StructSchema::Field> parents;
    std::vector<std::string> parent_tokens;

    capnp::StructSchema cur_reader = reader;

    for (const auto & field : sorted_fields)
    {
        if (field.tokens.empty())
            throw Exception("Logical error in CapnProtoRowInputStream", ErrorCodes::LOGICAL_ERROR);

        // Backtrack to common parent
        while (field.tokens.size() < parent_tokens.size() + 1
            || !std::equal(parent_tokens.begin(), parent_tokens.end(), field.tokens.begin()))
        {
            actions.push_back({Action::POP});
            parents.pop_back();
            parent_tokens.pop_back();

            if (parents.empty())
            {
                cur_reader = reader;
                break;
            }
            else
                cur_reader = parents.back().getType().asStruct();
        }

        // Go forward
        while (parent_tokens.size() + 1 < field.tokens.size())
        {
            const auto & token = field.tokens[parents.size()];
            auto node = getFieldOrThrow(cur_reader, token);
            if (node.getType().isStruct())
            {
                // Descend to field structure
                parents.emplace_back(node);
                parent_tokens.emplace_back(token);
                cur_reader = node.getType().asStruct();
                actions.push_back({Action::PUSH, node});
            }
            else if (node.getType().isList())
            {
                break; // Collect list
            }
            else
                throw Exception("Field " + token + " is neither Struct nor List", ErrorCodes::BAD_TYPE_OF_FIELD);
        }

        // Read field from the structure
        auto node = getFieldOrThrow(cur_reader, field.tokens[parents.size()]);
        if (node.getType().isList() && actions.size() > 0 && actions.back().field == node)
        {
            // The field list here flattens Nested elements into multiple arrays
            // In order to map Nested types in Cap'nProto back, they need to be collected
            // Since the field names are sorted, the order of field positions must be preserved
            // For example, if the fields are { b @0 :Text, a @1 :Text }, the `a` would come first
            // even though it's position is second.
            auto & columns = actions.back().columns;
            auto it = std::upper_bound(columns.cbegin(), columns.cend(), field.pos);
            columns.insert(it, field.pos);
        }
        else
        {
            actions.push_back({Action::READ, node, {field.pos}});
        }
    }
}

CapnProtoRowInputStream::CapnProtoRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSchemaInfo& info)
    : istr(istr_), header(header_), parser(std::make_shared<SchemaParser>())
{
    // Parse the schema and fetch the root object

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    auto schema = parser->impl.parseDiskFile(info.schemaPath(), info.absoluteSchemaPath(), {});
#pragma GCC diagnostic pop

    root = schema.getNested(info.messageName()).asStruct();

    /**
     * The schema typically consists of fields in various nested structures.
     * Here we gather the list of fields and sort them in a way so that fields in the same structure are adjacent,
     * and the nesting level doesn't decrease to make traversal easier.
     */
    NestedFieldList list;
    size_t num_columns = header.columns();
    for (size_t i = 0; i < num_columns; ++i)
        list.push_back(split(header, i));

    // Order list first by value of strings then by length of string vector.
    std::sort(list.begin(), list.end(), [](const NestedField & a, const NestedField & b) { return a.tokens < b.tokens; });
    createActions(list, root);
}

kj::Array<capnp::word> CapnProtoRowInputStream::readMessage()
{
    uint32_t segment_count;
    istr.readStrict(reinterpret_cast<char*>(&segment_count), sizeof(uint32_t));

    // one for segmentCount and one because segmentCount starts from 0
    const auto prefix_size = (2 + segment_count) * sizeof(uint32_t);
    const auto words_prefix_size = (segment_count + 1) / 2 + 1;
    auto prefix = kj::heapArray<capnp::word>(words_prefix_size);
    auto prefix_chars = prefix.asChars();
    ::memcpy(prefix_chars.begin(), &segment_count, sizeof(uint32_t));

    // read size of each segment
    for (size_t i = 0; i <= segment_count; ++i)
        istr.readStrict(prefix_chars.begin() + ((i + 1) * sizeof(uint32_t)), sizeof(uint32_t));

    // calculate size of message
    const auto expected_words = capnp::expectedSizeInWordsFromPrefix(prefix);
    const auto expected_bytes = expected_words * sizeof(capnp::word);
    const auto data_size = expected_bytes - prefix_size;
    auto msg = kj::heapArray<capnp::word>(expected_words);
    auto msg_chars = msg.asChars();

    // read full message
    ::memcpy(msg_chars.begin(), prefix_chars.begin(), prefix_size);
    istr.readStrict(msg_chars.begin() + prefix_size, data_size);

    return msg;
}

bool CapnProtoRowInputStream::read(MutableColumns & columns, RowReadExtension &)
{
    if (istr.eof())
        return false;

    auto array = readMessage();

#if CAPNP_VERSION >= 8000
    capnp::UnalignedFlatArrayMessageReader msg(array);
#else
    capnp::FlatArrayMessageReader msg(array);
#endif
    std::vector<capnp::DynamicStruct::Reader> stack;
    stack.push_back(msg.getRoot<capnp::DynamicStruct>(root));

    for (auto action : actions)
    {
        switch (action.type)
        {
            case Action::READ:
            {
                Field value = convertNodeToField(stack.back().get(action.field));
                if (action.columns.size() > 1)
                {
                    // Nested columns must be flattened into several arrays
                    // e.g. Array(Tuple(x ..., y ...)) -> Array(x ...), Array(y ...)
                    const Array & collected = DB::get<const Array &>(value);
                    size_t size = collected.size();
                    // The flattened array contains an array of a part of the nested tuple
                    Array flattened(size);
                    for (size_t column_index = 0; column_index < action.columns.size(); ++column_index)
                    {
                        // Populate array with a single tuple elements
                        for (size_t off = 0; off < size; ++off)
                        {
                            const TupleBackend & tuple = DB::get<const Tuple &>(collected[off]).toUnderType();
                            flattened[off] = tuple[column_index];
                        }
                        auto & col = columns[action.columns[column_index]];
                        col->insert(flattened);
                    }
                }
                else
                {
                    auto & col = columns[action.columns[0]];
                    col->insert(value);
                }

                break;
            }
            case Action::POP:
                stack.pop_back();
                break;
            case Action::PUSH:
                stack.push_back(stack.back().get(action.field).as<capnp::DynamicStruct>());
                break;
        }
    }

    return true;
}

void registerInputFormatCapnProto(FormatFactory & factory)
{
    factory.registerInputFormat(
        "CapnProto",
        [](ReadBuffer & buf, const Block & sample, const Context & context, UInt64 max_block_size, const FormatSettings & settings)
        {
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                std::make_shared<CapnProtoRowInputStream>(buf, sample, FormatSchemaInfo(context, "CapnProto")),
                sample,
                max_block_size,
                settings);
        });
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatCapnProto(FormatFactory &) {}
}

#endif // USE_CAPNP
