#pragma once

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <cstddef>
#include <exception>
#include <string>


namespace Poco
{
namespace Net
{

    class SocketImpl;

    class NonBlockingFilterBIO
    /// A filter BIO between OpenSSL and the transport socket, for the non-blocking calls of
    /// `SecureSocketImpl`. OpenSSL allows no I/O after `SSL_ERROR_WANT_WRITE` or a failed write
    /// (`SSL_get_error(3)`). In the non-blocking mode, the filter sends with `sendNow`, queues what
    /// does not fit, and drops the bytes after a send error, which the socket holds: OpenSSL never
    /// sees a pending or a failed write. In the blocking mode, it passes everything to the next BIO,
    /// after the queued bytes. Owned by its BIO; used under the mutex of the TLS socket.
    {
    public:
        static NonBlockingFilterBIO & install(SSL * ssl, SocketImpl & socket);
        /// Pushes a filter on top of the BIO of `ssl` after its handshake.

        class NonBlockingScope
        /// Switches the filter to the non-blocking mode for the lifetime of the object.
        {
        public:
            explicit NonBlockingScope(NonBlockingFilterBIO & filter_);
            ~NonBlockingScope();

            NonBlockingScope(const NonBlockingScope &) = delete;
            NonBlockingScope & operator=(const NonBlockingScope &) = delete;

        private:
            NonBlockingFilterBIO & filter;
        };

        bool sendQueued();
        /// Sends the queued bytes without waiting. Returns true when the queue is empty.

        bool hasQueued() const;

        void rethrowCaught();
        /// Throws the exception that a callback stored: it must not unwind through OpenSSL.

    private:
        explicit NonBlockingFilterBIO(SocketImpl & socket_);

        static BIO_METHOD * method();
        static int write(BIO * bio, const char * data, int length);
        static int read(BIO * bio, char * data, int length);
        static long ctrl(BIO * bio, int cmd, long num, void * ptr); // NOLINT(google-runtime-int): the signature of OpenSSL
        static int destroy(BIO * bio);

        int writeNonBlocking(const char * data, int length);
        int readNonBlocking(BIO * bio, char * data, int length);

        int writeQueuedToNext(BIO * bio);
        /// Returns 1 when the queue is empty, or -1 with the retry flags of the next BIO.

        static constexpr size_t maxQueued = 64 * 1024;
        /// A write over the limit fails the connection.

        void queueBytes(const char * data, size_t length);
        void dropQueue();

        SocketImpl & socket;
        bool nonBlocking = false;
        std::string queue;
        size_t queueOffset = 0;
        std::exception_ptr caught;
    };

}
}
