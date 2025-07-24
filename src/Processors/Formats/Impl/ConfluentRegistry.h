#pragma once

#include <cstdint>
#include "config.h"

#include <Common/CacheBase.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPCredentials.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/UUIDGenerator.h>

#if USE_AVRO
#    include <Compiler.hh>
#    include <DataFile.hh>
#    include <Decoder.hh>
#    include <Schema.hh>
#    include <ValidSchema.hh>
#endif

#if USE_PROTOBUF
#    include <google/protobuf/compiler/importer.h>
#    include <google/protobuf/descriptor.pb.h>
#    include <google/protobuf/dynamic_message.h>
#    include <google/protobuf/io/tokenizer.h>
#    include <google/protobuf/io/zero_copy_stream_impl.h>
#endif

namespace DB
{

class ConfluentSchemaRegistry
{
public:
    explicit ConfluentSchemaRegistry(const std::string & base_url_, const String & logger_name_, size_t schema_cache_max_size = 1000);

#if USE_AVRO
    avro::ValidSchema getAvroSchema(uint32_t id);
#endif

#if USE_PROTOBUF
    class ErrorCollector : public google::protobuf::compiler::MultiFileErrorCollector
    {
        void RecordError(absl::string_view filename, int line, int column, absl::string_view message) override;
    };

    const google::protobuf::Descriptor * getProtobufSchema(uint32_t id);
#endif

private:
#if USE_PROTOBUF
    google::protobuf::DescriptorPool independent_pool;
#endif
    Poco::UUIDGenerator generator;
    String fetchSchema(uint32_t id);

    Poco::URI base_url;

#if USE_AVRO
    CacheBase<uint32_t, avro::ValidSchema> avro_schema_cache;
#endif

#if USE_PROTOBUF
    CacheBase<uint32_t, const google::protobuf::Descriptor *> protobuf_schema_cache;
#endif

    String logger_name;
};


}
