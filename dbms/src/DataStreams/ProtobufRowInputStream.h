#pragma once
#include <Common/config.h>
/* #if USE_PROTOBUF */

#include <Core/Block.h>
#include <DataStreams/IRowInputStream.h>

/* #include <google/protobuf/descriptor.h> */
/* #include <google/protobuf/dynamic_message.h> */
/* #include <google/protobuf/io/zero_copy_stream_impl.h> */
/* #include <google/protobuf/io/tokenizer.h> */

#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/dynamic_message.h>

namespace DB
{

using namespace google::protobuf;
using namespace google::protobuf::compiler;

class ReadBuffer;

/**
 TODO: write an explaining comment
 */
class ProtobufRowInputStream : public IRowInputStream
{
public:
    ProtobufRowInputStream(
        ReadBuffer & istr_, const Block & header_,
        const String & schema_dir, const String & schema_file, const String & root_obj
    );

    bool read(MutableColumns & columns) override;

private:
    ReadBuffer & istr;
    Block header;
    Parser parser;
    const Message * prototype_msg;
    DescriptorPool descriptor_pool;
    DynamicMessageFactory message_factory;

    std::unordered_map<std::string, const FieldDescriptor *> name_to_field;
    static const std::unordered_map<FieldDescriptor::CppType, const std::string> protobuf_type_to_column_type_name;

    void validateSchema();
    void insertOneMessage(MutableColumns & columns, Message * mutable_msg);
};

}

/* #endif // USE_PROTOBUF */
