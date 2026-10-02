#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Common/tests/gtest_ephemeral_certificate.h>

#include <base/scope_guard.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Timespan.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <array>
#include <cerrno>
#include <exception>
#include <thread>
#include <typeinfo>


namespace
{

/// This custom `BIO` makes every TLS write fail as if the underlying socket was reset.
/// It is installed as the write `BIO` only, so the real read `BIO` and buffered application data remain intact.
int resetWrite(BIO * bio, const char *, int)
{
    BIO_clear_retry_flags(bio);
    errno = ECONNRESET;
    return -1;
}

long resetCtrl(BIO *, int command, long, void *) // NOLINT(google-runtime-int)
{
    if (command == BIO_CTRL_FLUSH)
        return 1;
    return 0;
}

int resetCreate(BIO * bio)
{
    BIO_set_init(bio, 1);
    BIO_set_data(bio, nullptr);
    return 1;
}

int resetDestroy(BIO *)
{
    return 1;
}

const BIO_METHOD * resetWriteBioMethod()
{
    /// `SSL_set0_wbio` transfers ownership of the `BIO` to `SSL`, so the method must support its full lifetime.
    /// A successful flush also lets OpenSSL perform routine `BIO` housekeeping without obscuring the write failure.
    static const BIO_METHOD * method = []
    {
        BIO_METHOD * result = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "reset-write");
        BIO_meth_set_write(result, resetWrite);
        BIO_meth_set_ctrl(result, resetCtrl);
        BIO_meth_set_create(result, resetCreate);
        BIO_meth_set_destroy(result, resetDestroy);
        return result;
    }();
    return method;
}

}


TEST(SSLSocketError, SendFailureDoesNotConsumePendingApplicationData)
{
    EphemeralCert cert;
    auto server_context = cert.makeContext(Poco::Net::Context::SERVER_USE);
    auto client_context = cert.makeContext(Poco::Net::Context::CLIENT_USE);

    Poco::Net::SecureServerSocket server_socket(Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_context);
    const Poco::Net::SocketAddress server_address("127.0.0.1", server_socket.address().port());
    const Poco::Timespan timeout(5, 0);

    /// Establish TCP before starting the server thread. The lazy TLS handshake is completed below,
    /// concurrently with the first server-side write.
    Poco::Net::SecureStreamSocket client(client_context);
    client.setLazyHandshake(true);
    client.connect(server_address);
    client.setSendTimeout(timeout);
    client.setReceiveTimeout(timeout);

    constexpr std::array<char, 8> payload{'p', 'e', 'n', 'd', 'i', 'n', 'g', '!'};
    std::exception_ptr server_exception;
    std::jthread server_thread([&]
    {
        try
        {
            auto accepted = server_socket.acceptConnection();
            accepted.setSendTimeout(timeout);
            accepted.setReceiveTimeout(timeout);
            if (accepted.sendBytes(payload.data(), static_cast<int>(payload.size())) != static_cast<int>(payload.size()))
                throw Poco::Net::NetException("Could not send the complete TLS test payload");
        }
        catch (...)
        {
            server_exception = std::current_exception();
        }
    });

    auto * client_impl = static_cast<Poco::Net::SecureStreamSocketImpl *>(client.impl());
    ASSERT_EQ(client_impl->completeHandshake(), 1);

    SSL * ssl = client_impl->ssl();
    ASSERT_NE(ssl, nullptr);

    /// The failing write `BIO` installed below would also reject a TLS shutdown write. Mark shutdown as complete
    /// during scope cleanup, while still letting `SSL` own and destroy the failing `BIO`.
    SCOPE_EXIT(SSL_set_shutdown(ssl, SSL_SENT_SHUTDOWN | SSL_RECEIVED_SHUTDOWN));

    /// `SSL_peek` processes the incoming TLS record without consuming its first application-data byte.
    /// Because the server sends the payload in one call, the complete payload must then be visible in `SSL_pending`.
    char first_byte = 0;
    const int peek_result = SSL_peek(ssl, &first_byte, 1);

    /// Wait until the server has completed its single write and propagate any server-side exception.
    server_thread.join();
    if (server_exception)
        std::rethrow_exception(server_exception);

    ASSERT_EQ(peek_result, 1);
    EXPECT_EQ(first_byte, payload.front());
    const int pending_before_send = SSL_pending(ssl);
    ASSERT_EQ(pending_before_send, static_cast<int>(payload.size()));

    /// Replace only the write `BIO`. The read `BIO` and application data already buffered by `SSL` are preserved,
    /// and `SSL_set0_wbio` transfers ownership of the failing `BIO` to `SSL`.
    BIO * reset_write_bio = BIO_new(resetWriteBioMethod());
    ASSERT_NE(reset_write_bio, nullptr);
    SSL_set0_wbio(ssl, reset_write_bio);

    /// OpenSSL requires the current thread's error queue to be empty before TLS I/O so that `SSL_get_error`
    /// classifies this `SSL_write` result rather than an earlier error. Check the exact exception type because
    /// the removed workaround also changed a connection reset into an `SSLException`.
    ERR_clear_error();
    errno = 0;
    const char byte_to_send = 'x';
    try
    {
        client.sendBytes(&byte_to_send, 1);
        ADD_FAILURE() << "Expected Poco::Net::ConnectionResetException";
    }
    catch (const Poco::Net::ConnectionResetException & exception)
    {
        EXPECT_EQ(typeid(exception), typeid(Poco::Net::ConnectionResetException));
    }
    catch (const Poco::Exception & exception)
    {
        ADD_FAILURE() << "Expected Poco::Net::ConnectionResetException, got " << exception.className();
    }
    catch (...)
    {
        /// Ok: this catch turns a non-Poco exception into a test failure.
        ADD_FAILURE() << "Expected Poco::Net::ConnectionResetException, got a non-Poco exception";
    }

    /// Previously `SecureSocketImpl::sendBytes` called `SSL_read` after this write failure, silently consuming
    /// one pending application-data byte. Handling the write error must leave all incoming data untouched.
    EXPECT_EQ(SSL_pending(ssl), pending_before_send);
}


#endif
