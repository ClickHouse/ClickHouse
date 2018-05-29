#include <Common/config.h>
/* #if USE_PROTOBUF */

#include <fcntl.h>
// TODO: remove
#include <iostream>

#include <Core/ColumnWithTypeAndName.h>
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

static std::unordered_map<std::string, const FieldDescriptor *> name_to_field;

static const std::unordered_map<FieldDescriptor::CppType, const std::string> protobuf_type_to_column_type = {
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

}

ProtobufRowInputStream::ProtobufRowInputStream(
    ReadBuffer & istr_, const Block & header_,
    const String & schema_dir, const String & schema_file, const String & root_obj
) : istr(istr_), header(header_)
{
    String schema_path = getSchemaPath(schema_dir, schema_file);
    // TODO: remove
    std::cerr << "schema_path: " << schema_path << std::endl;
    std::cerr << "root obj: " << root_obj << std::endl;

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

    // TODO: schema validation

    for (size_t field_i = 0; field_i != static_cast<size_t>(message_desc->field_count()); ++field_i)
    {
        const FieldDescriptor * field = message_desc->field(field_i);
        name_to_field[field->name()] = field;
    }

    for (size_t column_i = 0; column_i != header.columns(); ++column_i)
    {
        ColumnWithTypeAndName header_column = header.getByPosition(column_i);

        if (name_to_field.find(header_column.name) == name_to_field.end())
            throw Exception("Column \"" + header_column.name + "\" is not presented in a schema" /*, ErrorCodes::TODO*/);

        // TODO: remove
        std::cerr << "Column " << header_column.name << " has a type " << name_to_field[header_column.name]->type_name()
                  << "(" << name_to_field[header_column.name]->cpp_type() << ") in a schema" << std::endl;
    }

    // TODO: close(fd)?
}

// TODO: rename
void ProtobufRowInputStream::printMessageValues(Message * mutable_msg)
{
    const Reflection* reflection = mutable_msg->GetReflection();
    std::vector<const FieldDescriptor *> fields;
    reflection->ListFields(*mutable_msg, &fields);
    for (auto field_it = fields.begin(); field_it != fields.end(); ++field_it) {
        const FieldDescriptor * field = *field_it;
        if (field) {
            if ("name" == field->name())
            {
                String value = reflection->GetString(*mutable_msg, field);
                std::cout << field->name() << "(" << field->cpp_type() << ") -> " << value << std::endl;
            }
            else
            {
                uint32_t value = reflection->GetUInt32(*mutable_msg, field);
                std::cout << field->name() << "(" << field->cpp_type() << ") -> " << value << std::endl;
            }
            /* std::cout << field->name() << " -> " << std::endl; */
        } else
            std::cerr << "Error fieldDescriptor object is NULL" << std::endl;
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

    printMessageValues(mutable_msg);

    // TODO: remove
    istr.next();
    return columns[0]->getName()[0];

    return true; // TODO: why bool?
}

}

/* #endif // USE_PROTOBUF */
