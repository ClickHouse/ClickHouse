#include <Common/config.h>
#if USE_CAPNP

#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/CapnProtoRowInputFormat.h> // Y_IGNORE
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/FormatSchemaInfo.h>
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

CapnProtoRowInputFormat::NestedField split(const Block & header, size_t i)
{
    CapnProtoRowInputFormat::NestedField field = {{}, i};

    // Remove leading dot in field definition, e.g. ".msg" -> "msg"
    String name(header.safeGetByPosition(i).name);
    if (!name.empty() && name[0] == '.')
        name.erase(0, 1);

    boost::split(field.tokens, name, boost::is_any_of("._"));
    return field;
}


Field convertNodeToField(const capnp::DynamicValue::Reader & value)
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


void CapnProtoRowInputFormat::createActions(const NestedFieldList & sorted_fields, capnp::StructSchema reader)
{
    /// Columns in a table can map to fields in Cap'n'Proto or to structs.

    /// Store common parents and their tokens in order to backtrack.
    std::vector<capnp::StructSchema::Field> parents;
    std::vector<std::string> parent_tokens;

    capnp::StructSchema cur_reader = reader;

    for (const auto & field : sorted_fields)
    {
        if (field.tokens.empty())
            throw Exception("Logical error in CapnProtoRowInputFormat", ErrorCodes::LOGICAL_ERROR);

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
        if (node.getType().isList() && !actions.empty() && actions.back().field == node)
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

CapnProtoRowInputFormat::CapnProtoRowInputFormat(ReadBuffer & in, Block header, Params params, const FormatSchemaInfo & info)
    : IRowInputFormat(std::move(header), in, params), parser(std::make_shared<SchemaParser>())
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
    auto & sample = getPort().getHeader();
    NestedFieldList list;
    size_t num_columns = sample.columns();
    for (size_t i = 0; i < num_columns; ++i)
        list.push_back(split(sample, i));

    // Order list first by value of strings then by length of string vector.
    std::sort(list.begin(), list.end(), [](const NestedField & a, const NestedField & b) { return a.tokens < b.tokens; });
    createActions(list, root);
}


bool CapnProtoRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in.eof())
        return false;

    // Read from underlying buffer directly
    auto buf = in.buffer();
    auto base = reinterpret_cast<const capnp::word *>(in.position());

    // Check if there's enough bytes in the buffer to read the full message
    kj::Array<capnp::word> heap_array;
    auto array = kj::arrayPtr(base, buf.size() - in.offset());
    auto expected_words = capnp::expectedSizeInWordsFromPrefix(array);
    if (expected_words * sizeof(capnp::word) > array.size())
    {
        // We'll need to reassemble the message in a contiguous buffer
        heap_array = kj::heapArray<capnp::word>(expected_words);
        in.readStrict(heap_array.asChars().begin(), heap_array.asChars().size());
        array = heap_array.asPtr();
    }


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
                    const auto & collected = DB::get<const Array &>(value);
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

    // Advance buffer position if used directly
    if (heap_array.size() == 0)
    {
        auto parsed = (msg.getEnd() - base) * sizeof(capnp::word);
        in.position() += parsed;
    }

    return true;
}

void registerInputFormatProcessorCapnProto(FormatFactory & factory)
{
    factory.registerInputFormatProcessor(
        "CapnProto",
        [](ReadBuffer & buf, const Block & sample, const Context & context, IRowInputFormat::Params params, const FormatSettings &)
        {
            return std::make_shared<CapnProtoRowInputFormat>(buf, sample, params, FormatSchemaInfo(context, "capnp"));
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
