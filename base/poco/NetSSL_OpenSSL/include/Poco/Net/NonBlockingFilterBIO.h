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
    /// A filter BIO on top of the transport BIO of a TLS connection. `SecureSocketImpl` uses it for
    /// `sendNow`, `receiveNow` and `waitForSendOrReceive`.
    ///
    /// After `SSL_ERROR_WANT_WRITE`, or after a failed write, OpenSSL allows no other I/O on the
    /// connection (`SSL_get_error(3)`), so a response that the peer sent before it stopped reading
    /// could not be read. In the non-blocking mode, the filter sends and receives the encrypted bytes
    /// with `sendNow` and `receiveNow` of the transport socket. It queues what the socket does not
    /// accept, and it drops the bytes after a send error, which the socket holds. So OpenSSL never has
    /// a pending or a failed write, and `SSL_read` stays allowed.
    ///
    /// In the blocking mode, the filter passes everything to the next BIO, after the queued bytes, so
    /// that the stream keeps its order. A read sends the queued bytes too: a peer can wait for them
    /// before it answers.
    ///
    /// The BIO owns the object, and OpenSSL deletes it with the BIO. It is used under the mutex of the
    /// TLS socket.
    {
    public:
        static NonBlockingFilterBIO & install(SSL * ssl, SocketImpl & socket);
        /// Pushes a filter on top of the BIO of `ssl`, whose handshake is complete.
        /// `socket` is the transport socket of the connection.

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
        /// Sends the queued bytes without waiting, in the non-blocking mode.
        /// Returns true when no bytes are queued any more.

        bool hasQueued() const;
        /// Returns true when bytes are queued.

        void rethrowCaught();
        /// Throws the exception that a callback caught, if any. A callback must not let an exception
        /// unwind through OpenSSL, so it stores the exception and fails the operation.

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
        /// Writes the queued bytes to the next BIO. Returns 1 when no bytes are queued any more,
        /// or -1 when a write fails, with the retry flags of the next BIO.

        static constexpr size_t maxQueued = 64 * 1024;
        /// The callers keep the queue within one record and a few control records. The limit does
        /// not depend on that: a write over it fails the connection.

        void queueBytes(const char * data, size_t length);
        /// Queues the bytes. Throws `SSLException` when the queue would go over `maxQueued`.

        void dropQueue();

        SocketImpl & socket;
        bool nonBlocking = false;
        std::string queue;
        size_t queueOffset = 0;
        std::exception_ptr caught;
    };

}
}
