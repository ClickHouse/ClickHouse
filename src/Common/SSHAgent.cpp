#include <Common/SSHAgent.h>

#if USE_SSH

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <cstdlib>

namespace DB
{

namespace ErrorCodes
{
    extern const int SSH_AGENT_ERROR;
}

namespace
{

/// Message numbers of the agent protocol.
constexpr UInt8 SSH_AGENT_FAILURE = 5;
constexpr UInt8 SSH_AGENTC_REQUEST_IDENTITIES = 11;
constexpr UInt8 SSH_AGENT_IDENTITIES_ANSWER = 12;
constexpr UInt8 SSH_AGENTC_SIGN_REQUEST = 13;
constexpr UInt8 SSH_AGENT_SIGN_RESPONSE = 14;

/// A flag of a signature request, asking for a SHA-256 based signature algorithm for an RSA key (RFC 8332).
/// Without it, the agent produces a SHA-1 based signature, which is deprecated and is rejected by the server.
constexpr UInt32 SSH_AGENT_RSA_SHA2_256 = 2;

/// The agent is a local process, so its answers are trusted, but let's not allocate an unbounded amount of memory anyway.
constexpr UInt32 MAX_RESPONSE_SIZE = 256 * 1024;

/// The `SSHSIG` format, see https://github.com/openssh/openssh-portable/blob/master/PROTOCOL.sshsig
constexpr std::string_view SSHSIG_MAGIC_PREAMBLE = "SSHSIG";
constexpr UInt32 SSHSIG_VERSION = 1;
constexpr std::string_view SSHSIG_HASH_ALGORITHM = "sha256";
constexpr std::string_view SSHSIG_BEGIN = "-----BEGIN SSH SIGNATURE-----";
constexpr std::string_view SSHSIG_END = "-----END SSH SIGNATURE-----";
constexpr size_t SSHSIG_LINE_LENGTH = 76;

void appendUInt32(String & buffer, UInt32 value)
{
    for (size_t i = 0; i < sizeof(value); ++i)
        buffer.push_back(static_cast<char>((value >> (8 * (sizeof(value) - 1 - i))) & 0xFF));
}

/// The SSH wire format of a string: the length followed by the data.
void appendString(String & buffer, std::string_view value)
{
    appendUInt32(buffer, static_cast<UInt32>(value.size()));
    buffer.append(value);
}

/// Reads the fields of a message of the agent protocol.
class MessageReader
{
public:
    explicit MessageReader(std::string_view message_) : message(message_) { }

    UInt8 readUInt8()
    {
        check(sizeof(UInt8));
        return static_cast<UInt8>(message[pos++]);
    }

    UInt32 readUInt32()
    {
        check(sizeof(UInt32));
        UInt32 value = 0;
        for (size_t i = 0; i < sizeof(value); ++i)
            value = (value << 8) | static_cast<UInt8>(message[pos++]);
        return value;
    }

    std::string_view readString()
    {
        UInt32 size = readUInt32();
        check(size);
        std::string_view result = message.substr(pos, size);
        pos += size;
        return result;
    }

private:
    std::string_view message;
    size_t pos = 0;

    void check(size_t size) const
    {
        if (size > message.size() - pos)
            throw Exception(ErrorCodes::SSH_AGENT_ERROR, "Truncated response from the ssh-agent");
    }
};

void sendAll(Poco::Net::StreamSocket & socket, std::string_view data)
{
    size_t pos = 0;
    while (pos < data.size())
    {
        int sent = socket.sendBytes(data.data() + pos, static_cast<int>(data.size() - pos));
        if (sent <= 0)
            throw Exception(ErrorCodes::SSH_AGENT_ERROR, "Cannot send a request to the ssh-agent: it closed the connection");
        pos += sent;
    }
}

void receiveAll(Poco::Net::StreamSocket & socket, char * data, size_t size)
{
    size_t pos = 0;
    while (pos < size)
    {
        int received = socket.receiveBytes(data + pos, static_cast<int>(size - pos));
        if (received <= 0)
            throw Exception(ErrorCodes::SSH_AGENT_ERROR, "Cannot read a response from the ssh-agent: it closed the connection");
        pos += received;
    }
}

/// Sends one message to the agent and returns its response.
String talkToAgentImpl(std::string_view request, const String & socket_path)
{
    Poco::Net::StreamSocket socket(Poco::Net::SocketAddress(Poco::Net::SocketAddress::UNIX_LOCAL, socket_path));

    /// Every message is prefixed with its length.
    String framed_request;
    appendString(framed_request, request);
    sendAll(socket, framed_request);

    char length_buffer[sizeof(UInt32)];
    receiveAll(socket, length_buffer, sizeof(length_buffer));
    UInt32 length = MessageReader(std::string_view(length_buffer, sizeof(length_buffer))).readUInt32();

    if (length > MAX_RESPONSE_SIZE)
        throw Exception(ErrorCodes::SSH_AGENT_ERROR, "Too large response from the ssh-agent: {} bytes", length);

    String response(length, '\0');
    receiveAll(socket, response.data(), response.size());
    return response;
}

/// Every way in which the agent can turn out to be unusable - a socket that is not there, a name that is
/// too long for a Unix socket, a connection that breaks - is reported as one error code, because the caller
/// only has to decide whether to look for the key elsewhere.
String talkToAgent(std::string_view request, const String & socket_path)
{
    if (socket_path.empty())
        throw Exception(ErrorCodes::SSH_AGENT_ERROR, "There is no ssh-agent socket configured");

    try
    {
        return talkToAgentImpl(request, socket_path);
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::SSH_AGENT_ERROR, "Cannot talk to the ssh-agent at {}: {}", socket_path, e.displayText());
    }
}

/// Encodes the signature the way the `SSHSIG` format prescribes: base64 in a PEM-like envelope.
String armor(const String & blob)
{
    String base64 = base64Encode(blob);

    String result;
    result.append(SSHSIG_BEGIN);
    result.push_back('\n');
    for (size_t pos = 0; pos < base64.size(); pos += SSHSIG_LINE_LENGTH)
    {
        result.append(std::string_view(base64).substr(pos, SSHSIG_LINE_LENGTH));
        result.push_back('\n');
    }
    result.append(SSHSIG_END);
    return result;
}

}

String SSHAgent::getKeyType(const String & key_blob)
{
    return String(MessageReader(key_blob).readString());
}

String SSHAgent::getSocketPath()
{
    const char * socket_path = std::getenv("SSH_AUTH_SOCK"); // NOLINT(concurrency-mt-unsafe)
    return socket_path ? socket_path : "";
}

std::vector<SSHAgent::Identity> SSHAgent::listIdentities(const String & socket_path)
{
    String request;
    request.push_back(SSH_AGENTC_REQUEST_IDENTITIES);

    String response = talkToAgent(request, socket_path);
    MessageReader reader(response);

    UInt8 type = reader.readUInt8();
    if (type != SSH_AGENT_IDENTITIES_ANSWER)
        throw Exception(ErrorCodes::SSH_AGENT_ERROR,
            "The ssh-agent {} to list its keys", type == SSH_AGENT_FAILURE ? "refused" : "gave an unexpected response");

    UInt32 count = reader.readUInt32();
    std::vector<Identity> identities;
    for (UInt32 i = 0; i < count; ++i)
    {
        String key_blob{reader.readString()};
        String comment{reader.readString()};
        identities.push_back({.key_blob = std::move(key_blob), .comment = std::move(comment)});
    }

    return identities;
}

String SSHAgent::signString(const String & key_blob, std::string_view data, std::string_view sig_namespace, const String & socket_path)
{
    unsigned char hash[32];
    encodeSHA256(data, hash);

    /// What is signed is not the data itself, but this envelope around its hash.
    String data_to_sign;
    data_to_sign.append(SSHSIG_MAGIC_PREAMBLE); /// Without the length, unlike the fields below.
    appendString(data_to_sign, sig_namespace);
    appendString(data_to_sign, ""); /// Reserved.
    appendString(data_to_sign, SSHSIG_HASH_ALGORITHM);
    appendString(data_to_sign, std::string_view(reinterpret_cast<const char *>(hash), sizeof(hash)));

    UInt32 flags = 0;
    if (getKeyType(key_blob) == "ssh-rsa")
        flags |= SSH_AGENT_RSA_SHA2_256;

    String request;
    request.push_back(SSH_AGENTC_SIGN_REQUEST);
    appendString(request, key_blob);
    appendString(request, data_to_sign);
    appendUInt32(request, flags);

    String response = talkToAgent(request, socket_path);
    MessageReader reader(response);

    UInt8 type = reader.readUInt8();
    if (type != SSH_AGENT_SIGN_RESPONSE)
        throw Exception(ErrorCodes::SSH_AGENT_ERROR,
            "The ssh-agent {} to sign with the key of type {}",
            type == SSH_AGENT_FAILURE ? "refused" : "gave an unexpected response", getKeyType(key_blob));

    std::string_view signature = reader.readString();

    String signature_blob;
    signature_blob.append(SSHSIG_MAGIC_PREAMBLE);
    appendUInt32(signature_blob, SSHSIG_VERSION);
    appendString(signature_blob, key_blob);
    appendString(signature_blob, sig_namespace);
    appendString(signature_blob, ""); /// Reserved.
    appendString(signature_blob, SSHSIG_HASH_ALGORITHM);
    appendString(signature_blob, signature);

    return armor(signature_blob);
}

}

#endif
