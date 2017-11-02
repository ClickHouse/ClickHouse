#if USE_CAPNP

#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <DataStreams/CapnProtoRowInputStream.h>

#include <capnp/serialize.h>
#include <capnp/dynamic.h>
#include <boost/algorithm/string.hpp>
#include <boost/range/join.hpp>
#include <common/logger_useful.h>


namespace DB
{


CapnProtoRowInputStream::NestedField split(const Block & sample, size_t i)
{
    CapnProtoRowInputStream::NestedField field = {{}, i};

    // Remove leading dot in field definition, e.g. ".msg" -> "msg"
    String name(sample.safeGetByPosition(i).name);
    if (name.size() > 0 && name[0] == '.')
        name.erase(0, 1);

    boost::split(field.tokens, name, boost::is_any_of("."));
    return field;
}


Field convertNodeToField(capnp::DynamicValue::Reader value)
{
    switch (value.getType()) {
    case capnp::DynamicValue::UNKNOWN:
        throw Exception("Unknown field type");
    case capnp::DynamicValue::VOID:
        return Field();
    case capnp::DynamicValue::BOOL:
      return UInt64(value.as<bool>() ? 1 : 0);
    case capnp::DynamicValue::INT:
      return Int64((value.as<int64_t>()));
    case capnp::DynamicValue::UINT:
        return UInt64(value.as<uint64_t>());
    case capnp::DynamicValue::FLOAT:
      return Float64(value.as<double>());
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
        return UInt64(value.as<capnp::DynamicEnum>().getRaw());
    case capnp::DynamicValue::STRUCT:
        throw Exception("STRUCT type not supported, read individual fields instead");
    case capnp::DynamicValue::CAPABILITY:
        throw Exception("CAPABILITY type not supported");
    case capnp::DynamicValue::ANY_POINTER:
        throw Exception("ANY_POINTER type not supported");
	}
}

capnp::StructSchema::Field getFieldOrThrow(capnp::StructSchema node, const std::string & field)
{
    KJ_IF_MAYBE(child, node.findFieldByName(field))
        return *child;
    else
        throw Exception("Field " + field + " doesn't exist in schema.");
}

void CapnProtoRowInputStream::createActions(const NestedFieldList & sortedFields, capnp::StructSchema reader)
{
    String last;
    size_t level = 0;
    capnp::StructSchema::Field parent;
    
    for (const auto & field : sortedFields)
    {
        // Move to a different field in the same structure, keep parent
        if (level > 0 && field.tokens[level - 1] != last)
        {
            auto child = getFieldOrThrow(parent.getContainingStruct(), field.tokens[level - 1]);
            reader = child.getType().asStruct();
            actions.push_back({Action::POP});
            actions.push_back({Action::PUSH, child});
        }
        // Descend to a nested structure
        for (; level < field.tokens.size() - 1; ++level)
        {
            last = field.tokens[level];
            parent = getFieldOrThrow(reader, last);
            reader = parent.getType().asStruct();
            actions.push_back({Action::PUSH, parent});
        }
        // Read field from the structure
        actions.push_back({Action::READ, getFieldOrThrow(reader, field.tokens[level]), field.pos});
    }
}

CapnProtoRowInputStream::CapnProtoRowInputStream(ReadBuffer & istr_, const Block & sample_, const String & schema_file, const String & root_object)
    : istr(istr_), sample(sample_), parser(std::make_shared<SchemaParser>())
{
    // Parse the schema and fetch the root object
    auto schema = parser->impl.parseDiskFile(schema_file, schema_file, {});
    root = schema.getNested(root_object).asStruct();

    /**
     * The schema typically consists of fields in various nested structures.
     * Here we gather the list of fields and sort them in a way so that fields in the same structur are adjacent,
     * and the nesting level doesn't decrease to make traversal easier.
     */
    NestedFieldList list;
    size_t columns = sample.columns();
    for (size_t i = 0; i < columns; ++i)
        list.push_back(split(sample, i));

    // Reorder list to make sure we don't have to backtrack
    std::sort(list.begin(), list.end(), [](const NestedField & a, const NestedField & b)
    {
        if (a.tokens.size() == b.tokens.size())
            return a.tokens < b.tokens;
           return a.tokens.size() < b.tokens.size();
    });

    createActions(list, root);
}


bool CapnProtoRowInputStream::read(Block & block)
{
    if (istr.eof())
        return false;

    // Read from underlying buffer directly
    auto buf = istr.buffer();
    auto base = reinterpret_cast<const capnp::word *>(istr.position());
    
    // Check if there's enough bytes in the buffer to read the full message
    kj::Array<capnp::word> heap_array;
    auto array = kj::arrayPtr(base, buf.size() - istr.offset());
    auto expected_words = capnp::expectedSizeInWordsFromPrefix(array);
    if (expected_words * sizeof(capnp::word) > array.size())
    {
        // We'll need to reassemble the message in a contiguous buffer
        heap_array = kj::heapArray<capnp::word>(expected_words);
        istr.readStrict(heap_array.asChars().begin(), heap_array.asChars().size());
        array = heap_array.asPtr();
    }

    capnp::FlatArrayMessageReader msg(array);
    std::vector<capnp::DynamicStruct::Reader> stack;
    stack.push_back(msg.getRoot<capnp::DynamicStruct>(root));

    for (auto action : actions)
    {
        switch (action.type) {
        case Action::READ: {
            auto & col = block.getByPosition(action.column);
            Field value = convertNodeToField(stack.back().get(action.field));
            col.column->insert(value);
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
        istr.position() += parsed;
    }

    return true;
}

}

#endif
