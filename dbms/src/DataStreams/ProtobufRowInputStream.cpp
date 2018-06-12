#include <Common/config.h>
/* #if USE_PROTOBUF */

#include <fcntl.h>
// TODO: remove
#include <iostream>

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Common/escapeForFileName.h>
#include <IO/copyData.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBufferFromString.h>

/* #include <google/protobuf/compiler/parser.h> */
/* #include <google/protobuf/descriptor.h> */
/* #include <google/protobuf/dynamic_message.h> */
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
/* #include <google/protobuf/io/tokenizer.h> */

#include <DataStreams/ProtobufRowInputStream.h>

namespace DB
{

using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace google::protobuf::compiler;

namespace {

static String getSchemaPath(const String & schema_dir, const String & schema_file)
{
    return schema_dir + escapeForFileName(schema_file) + ".proto";
}

}

const std::unordered_map<FieldDescriptor::CppType, const std::string> ProtobufRowInputStream::protobuf_type_to_column_type_name = {
    // TODO: what if numeric values are always smaller than int32/64?
    {FieldDescriptor::CPPTYPE_INT32,  "Int32"},
    {FieldDescriptor::CPPTYPE_INT64,  "Int64"},
    {FieldDescriptor::CPPTYPE_UINT32, "UInt32"},
    {FieldDescriptor::CPPTYPE_UINT64, "UInt64"},
    {FieldDescriptor::CPPTYPE_DOUBLE, "Double"},
    {FieldDescriptor::CPPTYPE_FLOAT,  "Float"},
    {FieldDescriptor::CPPTYPE_BOOL,   "UInt8"},
    {FieldDescriptor::CPPTYPE_STRING, "String"}
    // TODO:
    /* {Enum, FieldDescriptor::CPPTYPE_ENUM}, */
    /* FieldDescriptor::CPPTYPE_MESSAGE, */
    /* FieldDescriptor::MAX_CPPTYPE */
};

void ProtobufRowInputStream::validateSchema()
{
    // TODO: support NULLs
    for (size_t column_i = 0; column_i != header.columns(); ++column_i)
    {
        ColumnWithTypeAndName header_column = header.getByPosition(column_i);

        if (name_to_field.find(header_column.name) == name_to_field.end())
            throw Exception("Column \"" + header_column.name + "\" is not presented in a schema" /*, ErrorCodes::TODO*/);

        const FieldDescriptor * field = name_to_field[header_column.name];
        FieldDescriptor::CppType protobuf_type = field->cpp_type();

        if (protobuf_type_to_column_type_name.find(protobuf_type) == protobuf_type_to_column_type_name.end())
        {
            throw Exception("Unsupported type " + std::string(field->type_name()) + " of a column " + header_column.name/*, ErrorCodes::TODO*/);
        }

        const std::string internal_type_name = protobuf_type_to_column_type_name.at(protobuf_type);
        const std::string column_type_name = header_column.type->getName();

        // TODO: can it be done with typeid_cast?
        if (internal_type_name != column_type_name)
        {
            throw Exception(
                "Input data type " + internal_type_name + " for column \"" + header_column.name + "\" "
                "is not compatible with a column type " + column_type_name/*, ErrorCodes::TODO*/
            );
        }
    }
}

// TODO: support Proto3

ProtobufRowInputStream::ProtobufRowInputStream(
    ReadBuffer & istr_, const Block & header_,
    const String & schema_dir, const String & schema_file, const String & root_obj
) : istr(istr_), header(header_)
{
    String schema_path = getSchemaPath(schema_dir, schema_file);

    // TODO: is it possible to use a high-level function with Protobuf?
    int fd = open(schema_path.c_str(), O_RDONLY);
    if (!fd)
        throw Exception("Failed to open a schema file " + schema_path/*, ErrorCodes::TODO*/);

    // TODO: catch and handle errors here and after
    ZeroCopyInputStream * file_input = new FileInputStream(fd);
    Tokenizer input(file_input, /*TODO: error_collector = */NULL);

    FileDescriptorProto file_desc_proto;
    if (!parser.Parse(&input, &file_desc_proto))
        throw Exception("Failed to parse a .proto schema " + schema_path/*, ErrorCodes::TODO*/);

    // TODO: is it needed?
    if (!file_desc_proto.has_name())
        file_desc_proto.set_name(root_obj);

    const FileDescriptor * file_desc = descriptor_pool.BuildFile(file_desc_proto);
    if (NULL == file_desc)
        throw Exception("Failed to construct a file descriptor from a file descriptor proto"/*, ErrorCodes::TODO*/);

    const Descriptor * message_desc = file_desc->FindMessageTypeByName(root_obj);
    if (NULL == message_desc)
        throw Exception("Failed to construct a message descriptor of " + root_obj + " from a file descriptor"/*, ErrorCodes::TODO*/);

    // TODO: will protobuf handle a pointer change? (memory management)
    prototype_msg = message_factory.GetPrototype(message_desc);
    if (NULL == prototype_msg)
        throw Exception("Failed to create a prototype message from a message descriptor"/*, ErrorCodes::TODO*/);

    for (size_t field_i = 0; field_i != static_cast<size_t>(message_desc->field_count()); ++field_i)
    {
        const FieldDescriptor * field = message_desc->field(field_i);
        name_to_field[field->name()] = field;
    }

    validateSchema();

    // TODO: close(fd)?
}

// TODO: get rid of code duplication

// TODO: some types require a bigger one to create a Field. Is it really how it should work?
#define FOR_PROTOBUF_CPP_TYPES(M)                          \
    M(FieldDescriptor::CPPTYPE_INT32,  Int64,   GetInt32)  \
    M(FieldDescriptor::CPPTYPE_INT64,  Int64,   GetInt64)  \
    M(FieldDescriptor::CPPTYPE_UINT32, UInt64,  GetUInt32) \
    M(FieldDescriptor::CPPTYPE_UINT64, UInt64,  GetUInt64) \
    M(FieldDescriptor::CPPTYPE_FLOAT,  Float64, GetFloat)  \
    M(FieldDescriptor::CPPTYPE_DOUBLE, Float64, GetDouble) \
    M(FieldDescriptor::CPPTYPE_STRING, String,  GetString) \
    // TODO:
    /* M(FieldDescriptor::CPPTYPE_BOOL,   UInt8)  \ */

    /* {Enum, FieldDescriptor::CPPTYPE_ENUM}, */
    /* FieldDescriptor::CPPTYPE_MESSAGE, */
    /* FieldDescriptor::MAX_CPPTYPE */

void ProtobufRowInputStream::insertOneMessage(MutableColumns & columns, Message * mutable_msg)
{
    const Reflection * reflection = mutable_msg->GetReflection();
    std::vector<const FieldDescriptor *> fields;
    reflection->ListFields(*mutable_msg, &fields);

    // TODO: what if a message has different fields order
    // TODO: what if there are more fields?
    // TODO: what if some field is not presented?


    // TODO: "Field" types intersect (naming)
    for (size_t field_i = 0; field_i != fields.size(); ++field_i)
    {
        const FieldDescriptor * field = fields[field_i];
        if (nullptr == field)
            throw Exception("FieldDescriptor for a column " + columns[field_i]->getName() + " is NULL"/*, ErrorCodes::TODO*/);

        // TODO: check field name?
        switch(field->cpp_type())
        {
#define DISPATCH(PROTOBUF_CPP_TYPE, CPP_TYPE, GETTER) \
        case PROTOBUF_CPP_TYPE: \
        { \
            const CPP_TYPE value = reflection->GETTER(*mutable_msg, field); \
            const Field field_value = value; \
            columns[field_i]->insert(field_value); \
            break; \
        }

        FOR_PROTOBUF_CPP_TYPES(DISPATCH);
#undef DISPATCH
        default:
            // TODO
            throw Exception("Unsupported type");
        }
    }
}


bool ProtobufRowInputStream::read(MutableColumns & columns)
{
    if (istr.eof())
        return false;

    // TODO: handle an array of messages, not just one message
    String file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(istr, file_buffer);
    }

    Message * mutable_msg = prototype_msg->New();
    if (NULL == mutable_msg)
        throw Exception("Failed to create a mutable message"/*, ErrorCodes::TODO*/);

    if (!mutable_msg->ParseFromArray(file_data.c_str(), file_data.size()))
        throw Exception("Failed to parse an input protobuf message"/*, ErrorCodes::TODO*/);

    insertOneMessage(columns, mutable_msg);

    return true;
}

}

/* #endif // USE_PROTOBUF */
