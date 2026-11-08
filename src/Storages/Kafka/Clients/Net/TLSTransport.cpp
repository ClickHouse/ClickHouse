#include <Storages/Kafka/Clients/Net/TLSTransport.h>

#include "config.h"

#if USE_SSL

#include <Poco/Exception.h>
#include <Poco/Net/Utility.h>
#include <Poco/Timespan.h>

#include <openssl/ssl.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>

#include <poll.h>

namespace Kafka::Net
{

namespace
{

/// pem_password_cb handing OpenSSL the configured key passphrase while the
/// private key file is loaded. Fails closed (-1) when no passphrase is bound.
int keyPasswordCallback(char * buf, int size, int /*rwflag*/, void * userdata)
{
    if (userdata == nullptr || size <= 0)
        return -1;
    const auto & password = *static_cast<const String *>(userdata);
    const size_t length = std::min(password.size(), static_cast<size_t>(size));
    std::memcpy(buf, password.data(), length);
    return static_cast<int>(length);
}

/// Loads the client certificate and its passphrase-protected private key.
/// This cannot go through Context::Params: Poco routes key passphrases to the
/// process-global SSLManager event machinery, and this Poco fork has neither
/// Poco::Crypto::EVPPKey nor Context::usePrivateKey/useCertificate, so the
/// OpenSSL API is used directly on the context.
void loadCertificateWithPassphrase(Poco::Net::Context & context, const TlsConfig & tls_config)
{
    SSL_CTX * ssl_context = context.sslContext();

    /// The certificate goes first so that OpenSSL checks the key against it
    /// inside SSL_CTX_use_PrivateKey_file (X509_R_KEY_VALUES_MISMATCH).
    if (!tls_config.certificate_location.empty()
        && SSL_CTX_use_certificate_chain_file(ssl_context, tls_config.certificate_location.c_str()) != 1)
        throw Exception(
            ErrorKind::Config,
            "Cannot load TLS certificate from '{}': {}",
            tls_config.certificate_location,
            Poco::Net::Utility::getLastError());

    String password = tls_config.key_password;
    SSL_CTX_set_default_passwd_cb(ssl_context, keyPasswordCallback);
    SSL_CTX_set_default_passwd_cb_userdata(ssl_context, &password);
    const int loaded = SSL_CTX_use_PrivateKey_file(ssl_context, tls_config.key_location.c_str(), SSL_FILETYPE_PEM);
    /// The callback must not outlive `password` (the userdata points into this
    /// frame); with a null userdata it fails closed if ever invoked again.
    SSL_CTX_set_default_passwd_cb_userdata(ssl_context, nullptr);
    if (loaded != 1)
        throw Exception(
            ErrorKind::Config,
            "Cannot load TLS private key from '{}': {}",
            tls_config.key_location,
            Poco::Net::Utility::getLastError());
}

}

TLSTransport::TLSTransport(std::shared_ptr<const WakeupSource> wakeup, TlsConfig tls_config_)
    : TCPTransport(std::move(wakeup))
    , tls_config(std::move(tls_config_))
{
}

Poco::Net::Context::Ptr TLSTransport::makeContext() const
{
    Poco::Net::Context::Params params;
    params.caLocation = tls_config.ca_location;
    /// Trust the system store only when no explicit CA bundle is configured.
    params.loadDefaultCAs = tls_config.ca_location.empty();
    params.verificationMode = tls_config.verify ? Poco::Net::Context::VERIFY_STRICT : Poco::Net::Context::VERIFY_NONE;

    const bool key_needs_passphrase = !tls_config.key_password.empty();
    if (key_needs_passphrase && tls_config.key_location.empty())
        throw Exception(ErrorKind::Config, "TLS key password is set, but the key location is empty");
    if (!key_needs_passphrase)
    {
        params.certificateFile = tls_config.certificate_location;
        params.privateKeyFile = tls_config.key_location;
    }

    Poco::Net::Context::Ptr context;
    try
    {
        context = new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, params);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorKind::Config, "Cannot create TLS context: {}", e.displayText());
    }

    /// Host-name verification on top of the chain verification (used by the
    /// explicit verifyPeerCertificate call after the handshake).
    context->enableExtendedCertificateVerification(tls_config.verify);

    if (key_needs_passphrase)
        loadCertificateWithPassphrase(*context, tls_config);

    return context;
}

void TLSTransport::connect(const String & host, UInt16 port, const Deadline & deadline)
{
    /// Plain TCP first: deadline and wakeup are observed there, and the
    /// socket ends up non-blocking.
    TCPTransport::connect(host, port, deadline);

    try
    {
        Poco::Net::Context::Ptr context = makeContext();

        /// Attaching to a non-blocking socket defers the handshake instead of
        /// performing it inline (the Connection::adoptSocket pattern). The
        /// host name is passed for SNI and the post-handshake host-name check.
        secure_socket = Poco::Net::SecureStreamSocket::attach(socket, host, context);

        /// The socket timeouts are Poco's handshake budget only when it drives
        /// the polling itself, i.e. on a blocking socket
        /// (SecureSocketImpl::getMaxTimeoutOrLimit, where 0 means "no limit" —
        /// hence the 1 ms floor). On this non-blocking socket they are a
        /// safety net; the waiting happens in waitReady below.
        static constexpr std::chrono::milliseconds max_handshake_budget{std::numeric_limits<int>::max()};
        const auto budget = std::max(std::chrono::milliseconds{1}, deadline.remainingClamped(max_handshake_budget));
        const Poco::Timespan handshake_timeout{budget.count() * 1000};
        secure_socket->setReceiveTimeout(handshake_timeout);
        secure_socket->setSendTimeout(handshake_timeout);

        /// NOTE: Poco would drive its own polling (blind to wakeup(), bounded
        /// only by the socket timeouts — the DNS-resolution trade-off) on a
        /// blocking socket. This socket is non-blocking, so completeHandshake
        /// yields the WANT_READ/WANT_WRITE sentinels instead, and the waiting
        /// happens in waitReady, which observes both the connect deadline and
        /// wakeup().
        for (;;)
        {
            int rc = 0;
            try
            {
                rc = secure_socket->completeHandshake();
            }
            catch (const Poco::TimeoutException &)
            {
                throw Exception(ErrorKind::Timeout, "TLS handshake with {}:{} timed out", host, port);
            }
            catch (const Poco::Exception & e)
            {
                throw Exception(ErrorKind::Connection, "TLS handshake with {}:{} failed: {}", host, port, e.displayText());
            }

            if (rc == 1)
                break;

            if (rc == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ)
                waitReady(POLLIN, deadline, "TLS handshake");
            else if (rc == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE)
                waitReady(POLLOUT, deadline, "TLS handshake");
            else if (rc == 0)
                throw Exception(ErrorKind::Connection, "TLS handshake with {}:{} failed: connection closed by broker", host, port);
            else
                throw Exception(ErrorKind::Connection, "Unexpected TLS handshake return {} from {}:{}", rc, host, port);
        }

        /// On a non-blocking socket completeHandshake skips the post-connect
        /// peer checks (SecureStreamSocket.h), so the host-name verification
        /// runs explicitly; the certificate chain itself was already verified
        /// during the handshake (VERIFY_STRICT).
        if (tls_config.verify)
        {
            try
            {
                secure_socket->verifyPeerCertificate();
            }
            catch (const Poco::Exception & e)
            {
                throw Exception(
                    ErrorKind::Connection, "TLS certificate verification for {}:{} failed: {}", host, port, e.displayText());
            }
        }

        secure_socket->setReceiveTimeout(Poco::Timespan(0));
        secure_socket->setSendTimeout(Poco::Timespan(0));
    }
    catch (...)
    {
        close();
        throw;
    }
}

size_t TLSTransport::readSome(char * data, size_t size, const Deadline & deadline)
{
    if (!secure_socket)
        throw Exception(ErrorKind::Connection, "TLS transport is not connected");

    const int to_read = static_cast<int>(std::min<size_t>(size, std::numeric_limits<int>::max()));
    for (;;)
    {
        int n = 0;
        try
        {
            n = secure_socket->receiveBytes(data, to_read);
        }
        catch (const Poco::TimeoutException &)
        {
            throw Exception(ErrorKind::Timeout, "Read timed out");
        }
        catch (const Poco::Exception & e)
        {
            /// The fatal-SSL path (bad record, unexpected reset, ...) throws.
            throw Exception(ErrorKind::Connection, "TLS read failed: {}", e.displayText());
        }

        if (n > 0)
            return static_cast<size_t>(n);
        if (n == 0)
            throw Exception(ErrorKind::Connection, "Connection closed by broker (TLS shutdown)");
        if (n == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ)
            waitReady(POLLIN, deadline, "Read");
        else if (n == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE)
            waitReady(POLLOUT, deadline, "Read");
        else
            throw Exception(ErrorKind::Connection, "Unexpected TLS read return {}", n);
    }
}

void TLSTransport::writeAll(std::string_view data, const Deadline & deadline)
{
    if (!secure_socket)
        throw Exception(ErrorKind::Connection, "TLS transport is not connected");

    size_t written = 0;
    while (written < data.size())
    {
        const int to_send = static_cast<int>(std::min<size_t>(data.size() - written, std::numeric_limits<int>::max()));
        int n = 0;
        try
        {
            n = secure_socket->sendBytes(data.data() + written, to_send);
        }
        catch (const Poco::TimeoutException &)
        {
            throw Exception(ErrorKind::Timeout, "Write timed out");
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorKind::Connection, "TLS write failed: {}", e.displayText());
        }

        if (n > 0)
        {
            written += static_cast<size_t>(n);
            continue;
        }

        /// A renegotiation can invert the directions: a write may need the
        /// socket readable and vice versa.
        if (n == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ)
            waitReady(POLLIN, deadline, "Write");
        else if (n == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE || n == 0)
            waitReady(POLLOUT, deadline, "Write");
        else
            throw Exception(ErrorKind::Connection, "Unexpected TLS write return {}", n);
    }
}

bool TLSTransport::readable()
{
    /// Data already decrypted and buffered inside OpenSSL does not show on
    /// the raw fd: available() is SSL_pending (SecureSocketImpl::available),
    /// so both the SSL buffer and the socket have to be probed.
    return secure_socket.has_value() && (secure_socket->impl()->available() > 0 || TCPTransport::readable());
}

void TLSTransport::close()
{
    if (secure_socket)
    {
        try
        {
            secure_socket->close();
        }
        catch (...) /// NOLINT(bugprone-empty-catch)
        {
            /// A failing TLS shutdown on a dead peer must not throw from
            /// close(); the socket is torn down below regardless.
        }
        secure_socket.reset();
    }
    TCPTransport::close();
}

bool TLSTransport::isOpen() const
{
    return secure_socket.has_value() ? secure_socket->impl()->initialized() : TCPTransport::isOpen();
}

}

#endif
