#include <Processors/Formats/Impl/ConfluentRegistry.h>

#include <IO/HTTPCommon.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>

namespace CurrentMetrics
{
    extern const Metric AvroSchemaCacheBytes;
    extern const Metric AvroSchemaCacheCells;
    extern const Metric ProtobufSchemaCacheBytes;
    extern const Metric ProtobufSchemaCacheCells;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}

ConfluentSchemaRegistry::ConfluentSchemaRegistry(
    const std::string & base_url_, const String & logger_name_, [[maybe_unused]] size_t schema_cache_max_size)
    : base_url(base_url_)
#if USE_AVRO
    , avro_schema_cache(CurrentMetrics::AvroSchemaCacheBytes, CurrentMetrics::AvroSchemaCacheCells, schema_cache_max_size)
#endif
#if USE_PROTOBUF
    , protobuf_schema_cache(CurrentMetrics::ProtobufSchemaCacheBytes, CurrentMetrics::ProtobufSchemaCacheCells, schema_cache_max_size)
#endif
    , logger_name(logger_name_)
{
    if (base_url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty Schema Registry URL");
}

#if USE_AVRO
avro::ValidSchema ConfluentSchemaRegistry::getAvroSchema(uint32_t id)
{
    auto [result_schema, loaded] = avro_schema_cache.getOrSet(
        id,
        [this, id]()
        {
            try
            {
                auto avro_schema = fetchSchema(id);
                return std::make_shared<avro::ValidSchema>(avro::compileJsonSchemaFromString(avro_schema));
            }
            catch (const avro::Exception & e)
            {
                throw Exception::createDeprecated(e.what(), ErrorCodes::INCORRECT_DATA);
            }
        });
    return *result_schema;
}
#endif

#if USE_PROTOBUF

void ConfluentSchemaRegistry::ErrorCollector::RecordError(absl::string_view filename, int line, int column, absl::string_view message)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Proto parse error at {} {}: {} {}", filename, line, column, message);
}

const google::protobuf::Descriptor * ConfluentSchemaRegistry::getProtobufSchema(uint32_t id)
{
    auto [schema, loaded] = protobuf_schema_cache.getOrSet(
        id,
        [this, id]()
        {
            auto schema_representation = fetchSchema(id);

            ErrorCollector error_collector;
            google::protobuf::compiler::DiskSourceTree source_tree;
            source_tree.MapPath("", ".");

            google::protobuf::compiler::SourceTreeDescriptorDatabase db(&source_tree);
            google::protobuf::compiler::Importer importer(&source_tree, &error_collector);

            std::istringstream // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                iss(schema_representation);
            google::protobuf::io::IstreamInputStream input_stream(&iss);
            google::protobuf::io::Tokenizer tokenizer(&input_stream, nullptr);
            google::protobuf::compiler::Parser parser;

            google::protobuf::FileDescriptorProto file_proto;

            if (!parser.Parse(&tokenizer, &file_proto))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to parse proto text");

            file_proto.set_name(generator.createRandom().toString() + ".proto");
            google::protobuf::FileDescriptorSet fds;
            *fds.add_file() = file_proto;

            return std::make_shared<const google::protobuf::Descriptor *>(independent_pool.BuildFile(file_proto)->message_type(0));
        });
    return *schema;
}

#endif

String ConfluentSchemaRegistry::fetchSchema(uint32_t id)
{
    std::cerr << "want to get schema " << id << '\n';
    try
    {
        try
        {
            Poco::URI url(base_url, base_url.getPath() + "/schemas/ids/" + std::to_string(id));
            LOG_TRACE((getLogger(logger_name)), "Fetching schema id = {} from url {}", id, url.toString());

            /// One second for connect/send/receive. Just in case.
            auto timeouts = ConnectionTimeouts().withConnectionTimeout(1).withSendTimeout(1).withReceiveTimeout(1);

            Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, url.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            request.setHost(url.getHost());

            if (!url.getUserInfo().empty())
            {
                Poco::Net::HTTPCredentials http_credentials;
                Poco::Net::HTTPBasicCredentials http_basic_credentials;

                http_credentials.fromUserInfo(url.getUserInfo());

                std::string decoded_username;
                Poco::URI::decode(http_credentials.getUsername(), decoded_username);
                std::cerr << "decoded_username " << decoded_username << '\n';
                http_basic_credentials.setUsername(decoded_username);

                if (!http_credentials.getPassword().empty())
                {
                    std::string decoded_password;
                    Poco::URI::decode(http_credentials.getPassword(), decoded_password);
                    std::cerr << "decoded_password " << decoded_password << '\n';
                    http_basic_credentials.setPassword(decoded_password);
                }

                http_basic_credentials.authenticate(request);
            }

            auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, url, timeouts);
            session->sendRequest(request);

            Poco::Net::HTTPResponse response;
            std::istream * response_body = receiveResponse(*session, request, response, false);

            Poco::JSON::Parser parser;
            auto json_body = parser.parse(*response_body).extract<Poco::JSON::Object::Ptr>();


            auto schema = json_body->getValue<std::string>("schema");
            LOG_TRACE((getLogger(logger_name)), "Successfully fetched schema id = {}\n{}", id, schema);
            return schema;
        }
        catch (const Exception &)
        {
            throw;
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(Exception::CreateFromPocoTag{}, e);
        }
    }
    catch (Exception & e)
    {
        e.addMessage("while fetching schema id = " + std::to_string(id));
        throw;
    }
}

}
