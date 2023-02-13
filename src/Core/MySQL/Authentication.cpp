#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Poco/RandomStream.h>
#include <Poco/SHA1Engine.h>
#include <Interpreters/Session.h>
#include <Access/Credentials.h>

#include <base/logger_useful.h>
#include <Common/OpenSSLHelpers.h>

#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
    extern const int MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES;
}

namespace MySQLProtocol
{

using namespace ConnectionPhase;

namespace Authentication
{

static const size_t SCRAMBLE_LENGTH = 20;

/** Generate a random string using ASCII characters but avoid separator character,
  * produce pseudo random numbers between with about 7 bit worth of entropty between 1-127.
  * https://github.com/mysql/mysql-server/blob/8.0/mysys/crypt_genhash_impl.cc#L427
  */
static String generateScramble()
{
    String scramble;
    scramble.resize(SCRAMBLE_LENGTH + 1, 0);
    Poco::RandomInputStream generator;

    for (size_t i = 0; i < SCRAMBLE_LENGTH; ++i)
    {
        generator >> scramble[i];
        scramble[i] &= 0x7f;
        if (scramble[i] == '\0' || scramble[i] == '$')
            scramble[i] = scramble[i] + 1;
    }

    return scramble;
}

Native41::Native41() : scramble(generateScramble()) { }

Native41::Native41(const String & password_, const String & scramble_)
{
    /// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
    /// SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
    Poco::SHA1Engine engine1;
    engine1.update(password_);
    const Poco::SHA1Engine::Digest & password_sha1 = engine1.digest();

    Poco::SHA1Engine engine2;
    engine2.update(password_sha1.data(), password_sha1.size());
    const Poco::SHA1Engine::Digest & password_double_sha1 = engine2.digest();

    Poco::SHA1Engine engine3;
    engine3.update(scramble_.data(), scramble_.size());
    engine3.update(password_double_sha1.data(), password_double_sha1.size());
    const Poco::SHA1Engine::Digest & digest = engine3.digest();

    scramble.resize(SCRAMBLE_LENGTH);
    for (size_t i = 0; i < SCRAMBLE_LENGTH; ++i)
        scramble[i] = static_cast<unsigned char>(password_sha1[i] ^ digest[i]);
}

void Native41::authenticate(
    const String & user_name, Session & session, std::optional<String> auth_response,
    std::shared_ptr<PacketEndpoint> packet_endpoint, bool, const Poco::Net::SocketAddress & address)
{
    if (!auth_response)
    {
        packet_endpoint->sendPacket(AuthSwitchRequest(getName(), scramble), true);
        AuthSwitchResponse response;
        packet_endpoint->receivePacket(response);
        auth_response = response.value;
    }

    if (auth_response->empty())
    {
        session.authenticate(user_name, "", address);
        return;
    }

    if (auth_response->size() != Poco::SHA1Engine::DIGEST_SIZE)
        throw Exception(
            "Wrong size of auth response. Expected: " + std::to_string(Poco::SHA1Engine::DIGEST_SIZE)
                + " bytes, received: " + std::to_string(auth_response->size()) + " bytes.",
            ErrorCodes::UNKNOWN_EXCEPTION);

    session.authenticate(MySQLNative41Credentials{user_name, scramble, *auth_response}, address);
}

#if USE_SSL

Sha256Password::Sha256Password(RSA & public_key_, RSA & private_key_, Poco::Logger * log_)
    : public_key(public_key_), private_key(private_key_), log(log_)
{
    /** Native authentication sent 20 bytes + '\0' character = 21 bytes.
     *  This plugin must do the same to stay consistent with historical behavior if it is set to operate as a default plugin. [1]
     *  https://github.com/mysql/mysql-server/blob/8.0/sql/auth/sql_authentication.cc#L3994
     */
    scramble = generateScramble();
}

void Sha256Password::authenticate(
    const String & user_name, Session & session, std::optional<String> auth_response,
    std::shared_ptr<PacketEndpoint> packet_endpoint, bool is_secure_connection, const Poco::Net::SocketAddress & address)
{
    if (!auth_response)
    {
        packet_endpoint->sendPacket(AuthSwitchRequest(getName(), scramble), true);

        if (packet_endpoint->in->eof())
            throw Exception("Client doesn't support authentication method " + getName() + " used by ClickHouse. Specifying user password using 'password_double_sha1_hex' may fix the problem.",
                            ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);

        AuthSwitchResponse response;
        packet_endpoint->receivePacket(response);
        auth_response.emplace(response.value);
        LOG_TRACE(log, "Authentication method mismatch.");
    }
    else
    {
        LOG_TRACE(log, "Authentication method match.");
    }

    bool sent_public_key = false;
    if (auth_response == "\1")
    {
        LOG_TRACE(log, "Client requests public key.");
        BIO * mem = BIO_new(BIO_s_mem());
        SCOPE_EXIT(BIO_free(mem));
        if (PEM_write_bio_RSA_PUBKEY(mem, &public_key) != 1)
        {
            throw Exception("Failed to write public key to memory. Error: " + getOpenSSLErrors(), ErrorCodes::OPENSSL_ERROR);
        }
        char * pem_buf = nullptr;
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wold-style-cast"
        int64_t pem_size = BIO_get_mem_data(mem, &pem_buf);
#    pragma GCC diagnostic pop
        String pem(pem_buf, pem_size);

        LOG_TRACE(log, "Key: {}", pem);

        AuthMoreData data(pem);
        packet_endpoint->sendPacket(data, true);
        sent_public_key = true;

        AuthSwitchResponse response;
        packet_endpoint->receivePacket(response);
        auth_response.emplace(response.value);
    }
    else
    {
        LOG_TRACE(log, "Client didn't request public key.");
    }

    String password;

    /** Decrypt password, if it's not empty.
     *  The original intention was that the password is a string[NUL] but this never got enforced properly so now we have to accept that
     *  an empty packet is a blank password, thus the check for auth_response.empty() has to be made too.
     *  https://github.com/mysql/mysql-server/blob/8.0/sql/auth/sql_authentication.cc#L4017
     */
    if (!is_secure_connection && !auth_response->empty() && auth_response != String("\0", 1))
    {
        LOG_TRACE(log, "Received nonempty password.");
        const auto & unpack_auth_response = *auth_response;
        const auto * ciphertext = reinterpret_cast<const unsigned char *>(unpack_auth_response.data());

        unsigned char plaintext[RSA_size(&private_key)];
        int plaintext_size = RSA_private_decrypt(unpack_auth_response.size(), ciphertext, plaintext, &private_key, RSA_PKCS1_OAEP_PADDING);
        if (plaintext_size == -1)
        {
            if (!sent_public_key)
                LOG_WARNING(log, "Client could have encrypted password with different public key since it didn't request it from server.");
            throw Exception("Failed to decrypt auth data. Error: " + getOpenSSLErrors(), ErrorCodes::OPENSSL_ERROR);
        }

        password.resize(plaintext_size);
        for (int i = 0; i < plaintext_size; ++i)
        {
            password[i] = plaintext[i] ^ static_cast<unsigned char>(scramble[i % SCRAMBLE_LENGTH]);
        }
    }
    else if (is_secure_connection)
    {
        password = *auth_response;
    }
    else
    {
        LOG_TRACE(log, "Received empty password");
    }

    if (!password.empty() && password.back() == 0)
    {
        password.pop_back();
    }

    session.authenticate(user_name, password, address);
}

#endif

}

}

}
