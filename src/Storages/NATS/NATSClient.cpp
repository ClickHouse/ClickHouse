#include <Storages/NATS/NATSClient.h>

#include <Common/Base64.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <IO/ReadHelpers.h>

#include "config.h"

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/String.h>

#if USE_SSL
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <openssl/evp.h>
#endif

#include <boost/algorithm/string/trim.hpp>

#include <algorithm>
#include <cstring>
#include <random>
#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONNECT_NATS;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace NATSClient
{

namespace
{

/// The longest line of the protocol that is accepted. `INFO` of a large cluster lists many URLs.
constexpr size_t MAX_LINE_SIZE = 4 * 1024 * 1024;

/// The servers do not allow a larger `max_payload`.
constexpr UInt64 MAX_PAYLOAD_LIMIT = 64 * 1024 * 1024;

/// Buffered output is sent once it reaches this size, and a larger buffer is released after it is sent.
constexpr size_t OUTPUT_BUFFER_SIZE = 1024 * 1024;

/// How many operations `processIncoming` reads at most in one call.
constexpr size_t MAX_OPERATIONS_PER_PROCESSING = 100000;

std::vector<std::string_view> splitTokens(std::string_view line)
{
    std::vector<std::string_view> tokens;
    size_t pos = 0;
    while (pos < line.size())
    {
        while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t'))
            ++pos;
        const size_t start = pos;
        while (pos < line.size() && line[pos] != ' ' && line[pos] != '\t')
            ++pos;
        if (pos > start)
            tokens.push_back(line.substr(start, pos - start));
    }
    return tokens;
}

UInt64 parseNumber(std::string_view token, std::string_view what)
{
    UInt64 value = 0;
    if (token.empty() || token.size() > 19 || !std::all_of(token.begin(), token.end(), isNumericASCII))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid {} '{}' in a NATS protocol line", what, token);
    for (char c : token)
        value = value * 10 + (c - '0');
    return value;
}

/// `NATS/1.0 408 Request Timeout` is the first line of the headers of a status message.
void parseStatus(std::string_view headers, Message & message)
{
    const auto end_of_line = headers.find("\r\n");
    std::string_view line = headers.substr(0, end_of_line);
    if (!line.starts_with("NATS/1.0"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid headers of a NATS message");
    line.remove_prefix(8);
    auto tokens = splitTokens(line);
    if (tokens.empty())
        return;
    if (tokens[0].size() == 3 && std::all_of(tokens[0].begin(), tokens[0].end(), isNumericASCII))
    {
        message.status = static_cast<UInt16>(parseNumber(tokens[0], "status"));
        const auto description_start = line.find(tokens[0]) + 3;
        String description(line.substr(description_start));
        boost::algorithm::trim(description);
        message.status_description = std::move(description);
    }
}

/// Subjects and queue groups are written into the lines of the protocol, so they must be single tokens:
/// otherwise a name with a space or a line break could add arguments or whole commands.
void checkToken(std::string_view token, std::string_view what, bool allow_empty)
{
    if (token.empty() && !allow_empty)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The NATS {} must not be empty", what);
    for (char c : token)
        if (static_cast<unsigned char>(c) <= ' ' || c == 0x7F)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The NATS {} '{}' must not contain whitespace or control characters", what, token);
}

String randomToken(size_t size)
{
    static constexpr std::string_view alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::uniform_int_distribution<size_t> distribution(0, alphabet.size() - 1);
    String result(size, ' ');
    for (auto & c : result)
        c = alphabet[distribution(thread_local_rng)];
    return result;
}

/// The value of the `index`-th block (`-----BEGIN ...-----`, then the value) of a NATS credentials file:
/// the user JWT is the first one, the NKey seed is the second one.
String getCredentialsBlock(std::string_view content, size_t index)
{
    size_t block = 0;
    bool in_block = false;
    size_t pos = 0;
    while (pos < content.size())
    {
        const size_t newline = std::min(content.find('\n', pos), content.size());
        String line(content.substr(pos, newline - pos));
        pos = newline + 1;
        boost::algorithm::trim(line);
        if (line.empty())
            continue;
        if (line.starts_with("---"))
        {
            if (line.contains("BEGIN"))
            {
                in_block = true;
                ++block;
            }
            else
                in_block = false;
            continue;
        }
        if (in_block && block == index + 1)
            return line;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid NATS credentials: no {}", index == 0 ? "user JWT" : "NKey seed");
}

/// CRC-16/XMODEM, as in `nkeys`.
UInt16 crc16(const unsigned char * data, size_t size)
{
    UInt16 crc = 0;
    for (size_t i = 0; i < size; ++i)
    {
        crc ^= static_cast<UInt16>(data[i]) << 8;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc & 0x8000) ? static_cast<UInt16>((crc << 1) ^ 0x1021) : static_cast<UInt16>(crc << 1);
    }
    return crc;
}

String base32Decode(std::string_view encoded)
{
    String result;
    UInt32 buffer = 0;
    int bits = 0;
    for (char c : encoded)
    {
        int value = 0;
        if (c >= 'A' && c <= 'Z')
            value = c - 'A';
        else if (c >= '2' && c <= '7')
            value = c - '2' + 26;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid NATS credentials: the NKey seed is not base32");
        buffer = (buffer << 5) | static_cast<UInt32>(value);
        bits += 5;
        if (bits >= 8)
        {
            bits -= 8;
            result.push_back(static_cast<char>((buffer >> bits) & 0xFF));
        }
    }
    return result;
}

/// Signs the nonce of the server with the Ed25519 key of an NKey seed, as the `nkeys` library does.
String signNonce(std::string_view seed, std::string_view nonce)
{
    const String decoded = base32Decode(seed);
    /// Two bytes of the prefix, 32 bytes of the key, two bytes of the checksum.
    if (decoded.size() != 36)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid NATS credentials: the NKey seed has a wrong size");
    const auto * bytes = reinterpret_cast<const unsigned char *>(decoded.data());
    const UInt16 checksum = static_cast<UInt16>(bytes[34] | (bytes[35] << 8));
    if (crc16(bytes, 34) != checksum)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid NATS credentials: wrong checksum of the NKey seed");
    /// The first five bits of a seed are the prefix `S`.
    if ((bytes[0] & 0xF8) != (18 << 3))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid NATS credentials: the NKey is not a seed");

#if USE_SSL
    EVP_PKEY * key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nullptr, bytes + 2, 32);
    if (!key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid NATS credentials: cannot load the Ed25519 key");
    EVP_MD_CTX * context = EVP_MD_CTX_new();
    unsigned char signature[64];
    size_t signature_size = sizeof(signature);
    const bool ok = context && EVP_DigestSignInit(context, nullptr, nullptr, nullptr, key) == 1
        && EVP_DigestSign(context, signature, &signature_size, reinterpret_cast<const unsigned char *>(nonce.data()), nonce.size()) == 1;
    EVP_MD_CTX_free(context);
    EVP_PKEY_free(key);
    if (!ok)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot sign the nonce of the NATS server");
    return base64Encode(String(reinterpret_cast<const char *>(signature), signature_size), /* url_encoding */ true, /* no_padding */ true);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "NATS credentials need SSL support, which ClickHouse was built without");
#endif
}

#if USE_SSL
Poco::Net::Context::Ptr makeTLSContext(const TLSSettings & tls)
{
    Poco::Net::Context::Params params;
    params.verificationMode = tls.verify_server ? Poco::Net::Context::VERIFY_RELAXED : Poco::Net::Context::VERIFY_NONE;
    params.caLocation = tls.ca_file;
    params.loadDefaultCAs = tls.ca_file.empty();
    params.certificateFile = tls.client_cert_file;
    params.privateKeyFile = tls.client_key_file;
    Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, params);
    /// The host name in the certificate is checked only when the certificate is verified at all.
    context->enableExtendedCertificateVerification(tls.verify_server);
    return context;
}
#endif

}


ServerAddress ServerAddress::parse(std::string_view url)
{
    ServerAddress address;
    String trimmed(url);
    boost::algorithm::trim(trimmed);
    std::string_view rest = trimmed;

    if (rest.starts_with("tls://"))
    {
        address.tls = true;
        rest.remove_prefix(6);
    }
    else if (rest.starts_with("nats://"))
    {
        rest.remove_prefix(7);
    }
    else if (rest.contains("://"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported scheme of the NATS server address");

    /// The user info ends at the last '@', as `validateNATSAddress` reads it: it may contain '/'.
    if (auto at = rest.rfind('@'); at != std::string_view::npos)
    {
        const std::string_view user_info = rest.substr(0, at);
        if (auto colon = user_info.find(':'); colon != std::string_view::npos)
        {
            address.user = String(user_info.substr(0, colon));
            address.password = SecretString(user_info.substr(colon + 1));
        }
        else
            address.token = SecretString(user_info);
        rest.remove_prefix(at + 1);
    }

    /// A path after the host is not used.
    rest = rest.substr(0, rest.find('/'));

    std::string_view host = rest;
    std::string_view port;
    if (rest.starts_with('['))
    {
        const auto bracket = rest.find(']');
        if (bracket == std::string_view::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid IPv6 address in the NATS server address");
        host = rest.substr(1, bracket - 1);
        if (bracket + 1 < rest.size())
        {
            if (rest[bracket + 1] != ':')
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid NATS server address");
            port = rest.substr(bracket + 2);
        }
    }
    else if (auto colon = rest.find(':'); colon != std::string_view::npos)
    {
        host = rest.substr(0, colon);
        port = rest.substr(colon + 1);
    }

    if (host.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No host in the NATS server address");
    address.host = String(host);

    if (!port.empty())
    {
        const UInt64 value = parseNumber(port, "port");
        if (value == 0 || value > std::numeric_limits<UInt16>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid port in the NATS server address");
        address.port = static_cast<UInt16>(value);
    }
    return address;
}

void validateTLSSettings([[maybe_unused]] const TLSSettings & tls)
{
#if USE_SSL
    /// Loaded one by one, to tell which of them is wrong.
    if (!tls.ca_file.empty())
    {
        TLSSettings ca_only;
        ca_only.ca_file = tls.ca_file;
        try
        {
            makeTLSContext(ca_only);
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot load NATS trusted CA certificates from {}: {}", tls.ca_file, e.displayText());
        }
    }

    if (!tls.client_cert_file.empty())
    {
        TLSSettings chain_only;
        chain_only.verify_server = false;
        chain_only.client_cert_file = tls.client_cert_file;
        chain_only.client_key_file = tls.client_key_file;
        try
        {
            makeTLSContext(chain_only);
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot load NATS client certificate chain from {} with key {}: {}",
                tls.client_cert_file, tls.client_key_file, e.displayText());
        }
    }
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "TLS connections to NATS are disabled, because ClickHouse was built without SSL support");
#endif
}


Connection::Connection(const ConnectionSettings & settings_, LoggerPtr log_)
    : settings(settings_)
    , log(std::move(log_))
{
    if (settings.servers.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No NATS servers to connect to");

    String errors;
    for (const auto & server : settings.servers)
    {
        try
        {
            connectToServer(server);
            last_ping = Clock::now();
            LOG_DEBUG(log, "Connected to NATS server {}", server_for_log);
            return;
        }
        catch (...)
        {
            output.clear();
            input.clear();
            input_pos = 0;
            socket.reset();
            error.clear();
            if (!errors.empty())
                errors += "; ";
            errors += fmt::format("{}:{}: {}", server.host, server.port, getCurrentExceptionMessage(false));
        }
    }

    throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to NATS: {}", errors);
}

Connection::~Connection() = default;

void Connection::connectToServer(const ServerAddress & server)
{
    server_for_log = fmt::format("{}:{}", server.host, server.port);

    const auto addresses = DNSResolver::instance().resolveAddressList(server.host, server.port);
    if (addresses.empty())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot resolve the host {}", server.host);
    for (size_t i = 0; i < addresses.size(); ++i)
    {
        try
        {
            /// A new socket for every address: a socket that failed to connect keeps the address family of that attempt.
            socket = std::make_unique<Poco::Net::StreamSocket>();
            socket->connect(addresses[i], settings.connect_timeout);
            break;
        }
        catch (...)
        {
            if (i + 1 == addresses.size())
                throw;
        }
    }

    socket->setReceiveTimeout(settings.request_timeout);
    socket->setSendTimeout(settings.request_timeout);
    socket->setNoDelay(true);
    socket->setBlocking(false);

    handshake(server);
}

void Connection::handshake(const ServerAddress & server)
{
    const auto deadline = Clock::now() + std::chrono::microseconds(settings.request_timeout.totalMicroseconds());

    const String info_line = readLine(deadline);
    if (!info_line.starts_with("INFO "))
        fail("the server did not start with INFO");

    {
        Poco::JSON::Parser parser;
        const auto info = parser.parse(info_line.substr(5)).extract<Poco::JSON::Object::Ptr>();
        if (info->has("max_payload"))
            max_payload = std::min<UInt64>(info->getValue<UInt64>("max_payload"), MAX_PAYLOAD_LIMIT);
        if (info->has("nonce"))
            nonce = info->getValue<String>("nonce");
        server_requires_tls = info->has("tls_required") && info->getValue<bool>("tls_required");
        const bool server_has_tls = server_requires_tls || (info->has("tls_available") && info->getValue<bool>("tls_available"));
        server_has_headers = info->has("headers") && info->getValue<bool>("headers");

        const bool want_tls = settings.tls.secure || server.tls;
        if (want_tls && !server_has_tls)
            fail("a secure connection is requested, but the server does not support TLS");
        if (want_tls || server_requires_tls)
            startTLS(server);
    }

    Poco::JSON::Object connect;
    connect.set("verbose", false);
    connect.set("pedantic", false);
    connect.set("tls_required", settings.tls.secure || server.tls || server_requires_tls);
    connect.set("name", "ClickHouse");
    connect.set("lang", "C++");
    connect.set("version", "1.0.0");
    connect.set("protocol", 1);
    connect.set("echo", true);
    if (server_has_headers)
    {
        connect.set("headers", true);
        connect.set("no_responders", true);
    }

    const auto & authentication = settings.authentication;
    if (!authentication.credentials.empty())
    {
        const std::string_view credentials = authentication.credentials;
        if (nonce.empty())
            fail("the server did not send a nonce to sign with the NATS credentials");
        connect.set("jwt", getCredentialsBlock(credentials, 0));
        connect.set("sig", signNonce(getCredentialsBlock(credentials, 1), nonce));
    }
    else if (!server.token.empty() || !authentication.token.empty())
    {
        connect.set("auth_token", String(std::string_view(!server.token.empty() ? server.token : authentication.token)));
    }
    else if (!server.user.empty())
    {
        connect.set("user", server.user);
        connect.set("pass", String(std::string_view(server.password)));
    }
    else if (!authentication.user.empty())
    {
        connect.set("user", authentication.user);
        connect.set("pass", String(std::string_view(authentication.password)));
    }

    std::ostringstream connect_json; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    connect.stringify(connect_json);
    writeLine("CONNECT " + connect_json.str());
    writeLine("PING");
    flushOutput();

    /// With `verbose` off the server answers only an error, so a `PONG` confirms that the connection is accepted.
    while (true)
    {
        const String line = readLine(deadline);
        if (line == "PONG")
            break;
        if (line.starts_with("-ERR"))
        {
            String message = line.substr(4);
            boost::algorithm::trim(message);
            boost::algorithm::trim_if(message, [](char c) { return c == '\''; });
            fail(message);
        }
        if (line == "PING")
        {
            writeLine("PONG");
            flushOutput();
        }
        /// `+OK` and `INFO` updates are not needed.
    }

    inbox_prefix = "_INBOX." + randomToken(22);
}

void Connection::startTLS([[maybe_unused]] const ServerAddress & server)
{
#if USE_SSL
    /// The server sends nothing after `INFO` until the client starts the TLS handshake, so nothing is buffered.
    output.clear();
    input.clear();
    input_pos = 0;

    /// The handshake is done in the blocking mode, where it is bounded by the socket timeouts.
    socket->setBlocking(true);
    auto secure_socket = std::make_unique<Poco::Net::SecureStreamSocket>(
        Poco::Net::SecureStreamSocket::attach(*socket, server.host, makeTLSContext(settings.tls)));
    socket = std::move(secure_socket);
    socket->setReceiveTimeout(settings.request_timeout);
    socket->setSendTimeout(settings.request_timeout);
    socket->setBlocking(false);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "TLS connections to NATS are disabled, because ClickHouse was built without SSL support");
#endif
}

void Connection::fail(const String & message)
{
    if (error.empty())
        error = message;
    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "NATS server {}: {}", server_for_log, message);
}

void Connection::checkUsable() const
{
    if (!error.empty())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "The connection to NATS server {} is not usable: {}", server_for_log, error);
}

void Connection::throwIfPermissionViolation()
{
    if (!permission_violation.empty())
        throw Exception(ErrorCodes::ACCESS_DENIED, "NATS server {}: {}", server_for_log, std::exchange(permission_violation, {}));
}

template <typename F>
auto Connection::guarded(F && f)
{
    checkUsable();
    try
    {
        return f();
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

void Connection::writeLine(std::string_view line)
{
    writeRaw(line.data(), line.size());
    writeRaw("\r\n", 2);
}

void Connection::writeRaw(const char * data, size_t size)
{
    output.append(data, size);
}

void Connection::flushOutput()
{
    const auto deadline = Clock::now() + std::chrono::microseconds(settings.request_timeout.totalMicroseconds());
    size_t pos = 0;
    while (pos < output.size())
    {
        const int rc = socket->sendBytes(output.data() + pos, static_cast<int>(std::min<size_t>(output.size() - pos, 1 << 20)));
        if (rc > 0)
        {
            pos += rc;
            continue;
        }

        /// Would block. TLS may need to read before it can write (renegotiation).
        int mode = Poco::Net::Socket::SELECT_WRITE;
#if USE_SSL
        if (rc == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ)
            mode = Poco::Net::Socket::SELECT_READ;
#endif
        const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(deadline - Clock::now()).count();
        if (remaining <= 0 || !socket->poll(Poco::Timespan(remaining), mode | Poco::Net::Socket::SELECT_ERROR))
            fail("timeout while writing to the server");
    }

    if (output.capacity() > 2 * OUTPUT_BUFFER_SIZE)
        String().swap(output);
    else
        output.clear();
}

bool Connection::waitForInput(Clock::time_point deadline)
{
    static constexpr size_t READ_SIZE = 65536;
    while (input_pos == input.size())
    {
        input.resize(READ_SIZE);
        input_pos = 0;
        const int rc = socket->receiveBytes(input.data(), static_cast<int>(READ_SIZE));
        input.resize(rc > 0 ? rc : 0);
        if (rc > 0)
            return true;
        if (rc == 0)
            fail("the server closed the connection");

        /// Would block: nothing complete has arrived yet (with TLS, possibly a part of a record).
        int mode = Poco::Net::Socket::SELECT_READ;
#if USE_SSL
        if (rc == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE)
            mode = Poco::Net::Socket::SELECT_WRITE;
#endif
        const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(deadline - Clock::now()).count();
        if (remaining <= 0 || !socket->poll(Poco::Timespan(remaining), mode | Poco::Net::Socket::SELECT_ERROR))
            return false;
    }
    return true;
}

void Connection::readExact(char * to, size_t size, Clock::time_point deadline)
{
    while (size)
    {
        if (!waitForInput(deadline))
            fail("timeout while reading from the server");
        const size_t bytes = std::min(size, input.size() - input_pos);
        memcpy(to, input.data() + input_pos, bytes);
        input_pos += bytes;
        to += bytes;
        size -= bytes;
    }
}

String Connection::readLine(Clock::time_point deadline)
{
    String line;
    while (true)
    {
        if (!waitForInput(deadline))
            fail("timeout while reading from the server");

        const char * begin = input.data() + input_pos;
        const char * end = input.data() + input.size();
        const char * newline = static_cast<const char *>(memchr(begin, '\n', end - begin));
        const char * stop = newline ? newline : end;
        if (line.size() + (stop - begin) > MAX_LINE_SIZE)
            fail("too long protocol line");
        line.append(begin, stop);
        input_pos = (newline ? newline + 1 : end) - input.data();
        if (newline)
            break;
    }

    if (line.ends_with('\r'))
        line.pop_back();
    return line;
}

bool Connection::readOperation(Clock::time_point start_deadline)
{
    if (!waitForInput(start_deadline))
        return false;

    /// Once an operation has started arriving, all of it must arrive within the request timeout.
    const auto deadline = Clock::now() + std::chrono::microseconds(settings.request_timeout.totalMicroseconds());
    const String line = readLine(deadline);
    const auto tokens = splitTokens(line);
    if (tokens.empty())
        return true;

    const String operation = Poco::toUpper(String(tokens[0]));

    if (operation == "MSG" || operation == "HMSG")
    {
        const bool has_headers = operation == "HMSG";
        /// MSG <subject> <sid> [reply-to] <#bytes>
        /// HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>
        const size_t numbers = has_headers ? 2 : 1;
        if (tokens.size() != 3 + numbers && tokens.size() != 4 + numbers)
            fail(fmt::format("invalid {} line", operation));

        Message message;
        message.subject = String(tokens[1]);
        message.sid = parseNumber(tokens[2], "sid");
        if (tokens.size() == 4 + numbers)
            message.reply = String(tokens[3]);

        const UInt64 total_size = parseNumber(tokens.back(), "size");
        const UInt64 header_size = has_headers ? parseNumber(tokens[tokens.size() - 2], "header size") : 0;
        if (header_size > total_size || total_size > max_payload + MAX_LINE_SIZE)
            fail(fmt::format("invalid size {} of a message, the maximum payload is {}", total_size, max_payload));

        String payload;
        payload.resize(total_size);
        readExact(payload.data(), total_size, deadline);
        char end[2];
        readExact(end, 2, deadline);
        if (end[0] != '\r' || end[1] != '\n')
            fail("a message does not end with CRLF");

        if (has_headers)
        {
            parseStatus(std::string_view(payload).substr(0, header_size), message);
            message.data = payload.substr(header_size);
        }
        else
            message.data = std::move(payload);

        if (replies_sid && message.sid == replies_sid)
            replies.push_back(std::move(message));
        else
            messages.push_back(std::move(message));
        return true;
    }

    if (operation == "PING")
    {
        writeLine("PONG");
        flushOutput();
        return true;
    }

    if (operation == "PONG")
    {
        ++pongs_received;
        return true;
    }

    if (operation == "-ERR")
    {
        String message = line.substr(4);
        boost::algorithm::trim(message);
        boost::algorithm::trim_if(message, [](char c) { return c == '\''; });
        /// The server keeps the connection after a permissions violation, but the operation has failed.
        if (message.starts_with("Permissions Violation"))
        {
            if (permission_violation.empty())
                permission_violation = message;
            return true;
        }
        fail(message);
    }

    if (operation == "+OK" || operation == "INFO")
        return true;

    fail(fmt::format("unknown operation {}", operation));
}

void Connection::sendPingIfNeeded()
{
    const auto now = Clock::now();
    if (now - last_ping < settings.ping_interval)
        return;

    if (pings_sent - pongs_received >= settings.max_pings_out)
        fail(fmt::format("the server did not answer {} PINGs", settings.max_pings_out));

    writeLine("PING");
    flushOutput();
    ++pings_sent;
    last_ping = now;
}

void Connection::processIncoming()
{
    guarded([&]
    {
        sendPingIfNeeded();
        const auto now = Clock::now();
        for (size_t i = 0; i < MAX_OPERATIONS_PER_PROCESSING; ++i)
        {
            if (!readOperation(now))
                break;
        }
    });
    throwIfPermissionViolation();
}

bool Connection::isUsable()
{
    if (!error.empty())
        return false;
    try
    {
        processIncoming();
    }
    catch (const Exception & e)
    {
        /// A permissions violation does not close the connection: it is reported by the next operation.
        if (e.code() == ErrorCodes::ACCESS_DENIED)
            permission_violation = e.message();
        else if (error.empty())
            error = e.message();
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
    }
    return error.empty();
}

void Connection::close()
{
    if (error.empty())
    {
        try
        {
            flushOutput();
        }
        catch (...)
        {
            LOG_DEBUG(log, "Error while closing the connection to NATS server {}: {}", server_for_log, getCurrentExceptionMessage(false));
        }
        error = "the connection is closed";
    }

    output.clear();
    try
    {
        if (socket)
            socket->close();
    }
    catch (...)
    {
        LOG_DEBUG(log, "Error while closing the socket of NATS server {}: {}", server_for_log, getCurrentExceptionMessage(false));
    }
}

UInt64 Connection::subscribe(const String & subject, const String & queue_group)
{
    checkToken(subject, "subject", /* allow_empty */ false);
    checkToken(queue_group, "queue group", /* allow_empty */ true);
    return guarded([&]
    {
        const UInt64 sid = next_sid++;
        if (queue_group.empty())
            writeLine(fmt::format("SUB {} {}", subject, sid));
        else
            writeLine(fmt::format("SUB {} {} {}", subject, queue_group, sid));
        return sid;
    });
}

void Connection::unsubscribe(UInt64 sid)
{
    guarded([&]
    {
        writeLine(fmt::format("UNSUB {}", sid));
        /// Messages of the subscription that are already buffered are not needed anymore.
        std::erase_if(messages, [sid](const Message & message) { return message.sid == sid; });
    });
}

void Connection::publish(const String & subject, const String & reply, std::string_view data)
{
    checkToken(subject, "subject", /* allow_empty */ false);
    checkToken(reply, "reply subject", /* allow_empty */ true);
    guarded([&]
    {
        if (data.size() > max_payload)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The message of size {} exceeds the maximum payload {} of NATS server {}",
                data.size(), max_payload, server_for_log);
        if (reply.empty())
            writeLine(fmt::format("PUB {} {}", subject, data.size()));
        else
            writeLine(fmt::format("PUB {} {} {}", subject, reply, data.size()));
        writeRaw(data.data(), data.size());
        writeRaw("\r\n", 2);
        if (output.size() >= OUTPUT_BUFFER_SIZE)
            flushOutput();
    });
}

void Connection::flush()
{
    guarded([&]
    {
        writeLine("PING");
        flushOutput();
        /// The server answers every `PING` in order, so this one is answered by the `PONG` with this number.
        const UInt64 expected_pongs = ++pings_sent;
        last_ping = Clock::now();

        const auto deadline = Clock::now() + std::chrono::microseconds(settings.request_timeout.totalMicroseconds());
        while (pongs_received < expected_pongs)
        {
            if (!readOperation(deadline))
                fail("timeout while waiting for PONG");
        }
    });
    /// The server answers in order, so an error about anything sent before the `PING` has arrived by now.
    throwIfPermissionViolation();
}

std::optional<Message> Connection::receive(Poco::Timespan timeout)
{
    if (messages.empty())
    {
        guarded([&]
        {
            const auto deadline = Clock::now() + std::chrono::microseconds(timeout.totalMicroseconds());
            flushOutput();
            while (messages.empty())
            {
                sendPingIfNeeded();
                const auto next_ping = last_ping + settings.ping_interval;
                readOperation(std::min(deadline, next_ping));
                if (messages.empty() && Clock::now() >= deadline)
                    break;
            }

            /// Read what has arrived already as well: data left in the socket is lost if the server resets the connection,
            /// while received messages are kept by `takeReceived`.
            const auto now = Clock::now();
            for (size_t i = 0; i < MAX_OPERATIONS_PER_PROCESSING; ++i)
            {
                if (!readOperation(now))
                    break;
            }
        });
    }

    throwIfPermissionViolation();

    if (messages.empty())
        return {};
    auto message = std::move(messages.front());
    messages.pop_front();
    return message;
}

String Connection::newInbox()
{
    return fmt::format("{}.{}", inbox_prefix, next_inbox++);
}

Message Connection::request(const String & subject, std::string_view data, Poco::Timespan timeout)
{
    if (!replies_sid)
        replies_sid = subscribe(inbox_prefix + ".*", "");

    const String reply_subject = newInbox();
    publish(subject, reply_subject, data);

    auto reply = guarded([&]() -> std::optional<Message>
    {
        flushOutput();
        const auto deadline = Clock::now() + std::chrono::microseconds(timeout.totalMicroseconds());
        while (true)
        {
            /// Replies to earlier requests that timed out are not needed anymore.
            while (!replies.empty())
            {
                auto message = std::move(replies.front());
                replies.pop_front();
                if (message.subject == reply_subject)
                    return message;
            }

            if (Clock::now() >= deadline)
                return {};

            sendPingIfNeeded();
            readOperation(std::min(deadline, last_ping + settings.ping_interval));
        }
    });

    throwIfPermissionViolation();

    /// The connection is still usable after a timeout: the reply may just come late.
    if (!reply)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded while waiting for a reply from NATS server {}", server_for_log);
    return std::move(*reply);
}

}

}
