#include <Storages/ArrowFlight/ArrowFlightConnection.h>

#if USE_ARROWFLIGHT
#include <algorithm>
#include <limits>
#include <Common/Base64.h>
#include <Poco/String.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARROWFLIGHT_CONNECTION_FAILURE;
}

ArrowFlightConnection::ArrowFlightConnection(const StorageArrowFlight::Configuration & config)
    : host(config.host)
    , port(config.port)
    , use_basic_authentication(config.use_basic_authentication)
    , username(config.username)
    , password(config.password)
    , enable_ssl(config.enable_ssl)
    , ssl_ca(config.ssl_ca)
    , ssl_override_hostname(config.ssl_override_hostname)
{
}

void ArrowFlightConnection::setDeadline(grpc::ClientContext & context, UInt64 timeout_sec)
{
    /// Zero means no deadline.
    if (timeout_sec == 0)
        return;

    /// The deadline is now() + timeout; half of the range of the clock is left for now(), which still keeps every
    /// value a caller can mean.
    static constexpr UInt64 max_timeout_sec = static_cast<UInt64>(std::numeric_limits<Int64>::max() / 2 / 1'000'000'000);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(std::min(timeout_sec, max_timeout_sec)));
}

String ArrowFlightConnection::formatError(const grpc::Status & status, const grpc::ClientContext & context)
{
    /// A server of Arrow Flight sends the original status in the trailing metadata, which the client of the Apache
    /// Arrow library restores (`FromGrpcContext`).
    const auto & trailers = context.GetServerTrailingMetadata();
    auto find = [&](std::string_view name) -> std::optional<String>
    {
        auto it = trailers.find(grpc::string_ref(name.data(), name.size()));
        if (it == trailers.end())
            return std::nullopt;
        return String(it->second.data(), it->second.size());
    };
    if (auto code = find("x-arrow-status"))
    {
        static const std::unordered_map<String, String> code_names = { // STYLE_CHECK_ALLOW_STD_CONTAINERS
            {"1", "Out of memory"}, {"2", "Key error"}, {"3", "Type error"}, {"4", "Invalid"}, {"5", "IOError"},
            {"6", "Capacity error"}, {"7", "Index error"}, {"8", "Cancelled"}, {"9", "Unknown error"},
            {"10", "NotImplemented"}, {"11", "Serialization error"}, {"42", "ExecutionError in Gandiva"},
            {"45", "Already exists"}};
        auto name = code_names.find(*code);
        String result = (name == code_names.end() ? String("Unknown") : name->second) + ": "
            + find("x-arrow-status-message-bin").value_or(status.error_message());
        if (auto detail = find("x-arrow-status-detail-bin"))
            result += ". Detail: " + *detail;
        return result;
    }

    /// As `arrow::flight::TransportStatus::ToStatus` for the gRPC code.
    const String & message = status.error_message();
    switch (status.error_code())
    {
        case grpc::StatusCode::OK:
            return "OK";
        case grpc::StatusCode::CANCELLED:
            return "Cancelled: Flight cancelled call, with message: " + message + ". Detail: Cancelled";
        case grpc::StatusCode::UNKNOWN:
            return "Unknown error: Flight RPC failed with message: " + message + ". Detail: Failed";
        case grpc::StatusCode::INVALID_ARGUMENT:
        case grpc::StatusCode::OUT_OF_RANGE:
            return "Invalid: Flight returned invalid argument error, with message: " + message;
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            return "IOError: Flight returned timeout error, with message: " + message + ". Detail: TimedOut";
        case grpc::StatusCode::NOT_FOUND:
            return "Key error: Flight returned not found error, with message: " + message;
        case grpc::StatusCode::ALREADY_EXISTS:
            return "Already exists: Flight returned already exists error, with message: " + message;
        case grpc::StatusCode::PERMISSION_DENIED:
            return "IOError: Flight returned unauthorized error, with message: " + message + ". Detail: Unauthorized";
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
        case grpc::StatusCode::FAILED_PRECONDITION:
        case grpc::StatusCode::ABORTED:
        case grpc::StatusCode::UNAVAILABLE:
            return "IOError: Flight returned unavailable error, with message: " + message + ". Detail: Unavailable";
        case grpc::StatusCode::UNIMPLEMENTED:
            return "NotImplemented: Flight returned unimplemented error, with message: " + message;
        case grpc::StatusCode::INTERNAL:
        case grpc::StatusCode::DATA_LOSS:
            return "IOError: Flight returned internal error, with message: " + message + ". Detail: Internal";
        case grpc::StatusCode::UNAUTHENTICATED:
            return "IOError: Flight returned unauthenticated error, with message: " + message + ". Detail: Unauthenticated";
        default:
            return fmt::format("Unknown error: gRPC failed with error code {} and message: {}", static_cast<int>(status.error_code()), message);
    }
}

std::shared_ptr<ArrowFlightConnection::Stub> ArrowFlightConnection::getStub(UInt64 timeout_sec) const
{
    connect(timeout_sec);

    std::lock_guard lock{mutex};
    return stub;
}

void ArrowFlightConnection::prepareCall(grpc::ClientContext & context, UInt64 timeout_sec) const
{
    connect(timeout_sec);

    setDeadline(context, timeout_sec);
    std::lock_guard lock{mutex};
    if (!authorization.empty())
        context.AddMetadata("authorization", String(authorization));
}

void ArrowFlightConnection::connect(UInt64 timeout_sec) const
{
    {
        std::lock_guard lock{mutex};
        if (stub)
            return;
    }

    /// An IPv6 address is enclosed in brackets in a gRPC target.
    const String target = (host.contains(':') && !host.starts_with('[')) ? fmt::format("[{}]:{}", host, port) : fmt::format("{}:{}", host, port);

    grpc::ChannelArguments arguments;
    /// As the Arrow Flight client: a message is not limited in size.
    arguments.SetMaxReceiveMessageSize(-1);
    arguments.SetMaxSendMessageSize(-1);

    std::shared_ptr<grpc::ChannelCredentials> credentials;
    if (enable_ssl)
    {
        grpc::SslCredentialsOptions options;
        if (!ssl_ca.empty())
            options.pem_root_certs = loadCertificate(ssl_ca);
        if (!ssl_override_hostname.empty())
            arguments.SetSslTargetNameOverride(ssl_override_hostname);
        credentials = grpc::SslCredentials(options);
    }
    else
    {
        credentials = grpc::InsecureChannelCredentials();
    }

    auto new_stub = arrow::flight::protocol::FlightService::NewStub(grpc::CreateCustomChannel(target, credentials, arguments));

    String new_authorization;
    if (use_basic_authentication)
    {
        /// As `arrow::flight::FlightClient::AuthenticateBasicToken`: a handshake with the credentials in the header,
        /// which the server answers with the token in the `authorization` header.
        grpc::ClientContext context;
        setDeadline(context, timeout_sec);
        context.AddMetadata("authorization", "Basic " + base64Encode(username + ":" + String(password)));
        auto stream = new_stub->Handshake(&context);
        stream->WritesDone();
        arrow::flight::protocol::HandshakeResponse response;
        while (stream->Read(&response))
        {
        }
        auto status = stream->Finish();
        if (!status.ok())
            throw Exception(
                ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE, "Failed to authenticate Arrow Flight server: {}", formatError(status, context));

        for (const auto * metadata : {&context.GetServerInitialMetadata(), &context.GetServerTrailingMetadata()})
        {
            for (const auto & [key, value] : *metadata)
            {
                String name(key.data(), key.size());
                String header_value(value.data(), value.size());
                if (Poco::toLower(name) == "authorization" && Poco::toLower(header_value).starts_with("bearer "))
                    new_authorization = header_value;
            }
        }
    }

    std::lock_guard lock{mutex};

    /// Published only if nobody published first: every query has to see the same authenticated connection.
    if (!stub)
    {
        stub = std::move(new_stub);
        authorization = std::move(new_authorization);
    }
}

String ArrowFlightConnection::loadCertificate(const String & path)
{
    ReadBufferFromFile buf{path};
    String str;
    readStringUntilEOF(str, buf);
    buf.close();
    return str;
}

std::shared_ptr<ArrowFlightConnection> ArrowFlightConnection::clone() const
{
    return std::shared_ptr<ArrowFlightConnection>{new ArrowFlightConnection(*this)};
}

std::shared_ptr<ArrowFlightConnection> ArrowFlightConnection::cloneWithHostAndPort(const String & host_, int port_) const
{
    auto res = clone();
    res->host = host_;
    res->port = port_;
    return res;
}

ArrowFlightConnection::ArrowFlightConnection(const ArrowFlightConnection & src)
    : host(src.host)
    , port(src.port)
    , use_basic_authentication(src.use_basic_authentication)
    , username(src.username)
    , password(src.password)
    , enable_ssl(src.enable_ssl)
    , ssl_ca(src.ssl_ca)
    , ssl_override_hostname(src.ssl_override_hostname)
{
}

}

#endif
