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
    SCOPE_EXIT(SSL_set_shutdown(ssl, SSL_SENT_SHUTDOWN | SSL_RECEIVED_SHUTDOWN));

    char first_byte = 0;
    const int peek_result = SSL_peek(ssl, &first_byte, 1);

    server_thread.join();
    if (server_exception)
        std::rethrow_exception(server_exception);

    ASSERT_EQ(peek_result, 1);
    EXPECT_EQ(first_byte, payload.front());
    const int pending_before_send = SSL_pending(ssl);
    ASSERT_EQ(pending_before_send, static_cast<int>(payload.size()));

    BIO * reset_write_bio = BIO_new(resetWriteBioMethod());
    ASSERT_NE(reset_write_bio, nullptr);
    SSL_set0_wbio(ssl, reset_write_bio);

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
        ADD_FAILURE() << "Expected Poco::Net::ConnectionResetException, got a non-Poco exception";
    }

    EXPECT_EQ(SSL_pending(ssl), pending_before_send);
}


#endif
