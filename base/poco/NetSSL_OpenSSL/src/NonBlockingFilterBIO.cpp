#include "Poco/Net/NonBlockingFilterBIO.h"
#include "Poco/Net/SocketImpl.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Exception.h"

#include <algorithm>
#include <climits>
#include <memory>


namespace Poco {
namespace Net {


NonBlockingFilterBIO::NonBlockingFilterBIO(SocketImpl& socket_):
	socket(socket_)
{
}


NonBlockingFilterBIO& NonBlockingFilterBIO::install(SSL* ssl, SocketImpl& socket)
{
	BIO* transport = SSL_get_rbio(ssl);
	if (!transport || transport != SSL_get_wbio(ssl))
		throw IllegalStateException("A TLS connection gets a filter BIO only when one BIO serves both directions");
	if (!SSL_is_init_finished(ssl))
		throw IllegalStateException("A TLS connection gets a filter BIO only after the handshake");

	std::unique_ptr<NonBlockingFilterBIO> owned(new NonBlockingFilterBIO(socket));
	BIO* filter = BIO_new(method());
	if (!filter)
		throw SSLException("Cannot create BIO object");
	NonBlockingFilterBIO& result = *owned;
	BIO_set_data(filter, owned.release());
	BIO_set_init(filter, 1);
	BIO_push(filter, transport);

	/// `SSL_set0_rbio` and `SSL_set0_wbio` each take one reference of the filter. They also free
	/// each reference to the transport BIO that the connection and its record layers hold, so the
	/// chain takes one more: the transport BIO stays alive as the next BIO of the filter, and the
	/// last `BIO_free_all` of the filter frees both.
	BIO_up_ref(filter);
	BIO_up_ref(transport);
	SSL_set0_rbio(ssl, filter);
	SSL_set0_wbio(ssl, filter);

	/// `SecureSocketImpl::sendNow` repeats an `SSL_write` that wants to read with the same data,
	/// which the caller can pass at another address.
	SSL_set_mode(ssl, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
	return result;
}


NonBlockingFilterBIO::NonBlockingScope::NonBlockingScope(NonBlockingFilterBIO& filter_):
	filter(filter_)
{
	filter.nonBlocking = true;
}


NonBlockingFilterBIO::NonBlockingScope::~NonBlockingScope()
{
	filter.nonBlocking = false;
}


bool NonBlockingFilterBIO::sendQueued()
{
	while (hasQueued())
	{
		const size_t left = queue.size() - queueOffset;
		const int sent = socket.sendNow(queue.data() + queueOffset, static_cast<int>(std::min<size_t>(left, INT_MAX)));
		if (sent < 0)
		{
			/// The socket holds the error, and the connection cannot send any more.
			dropQueue();
			return true;
		}
		if (sent == 0)
			return false;
		queueOffset += static_cast<size_t>(sent);
	}
	dropQueue();
	return true;
}


bool NonBlockingFilterBIO::hasQueued() const
{
	return queueOffset < queue.size();
}


void NonBlockingFilterBIO::rethrowCaught()
{
	if (!caught)
		return;
	std::exception_ptr exception = caught;
	caught = nullptr;
	std::rethrow_exception(exception);
}


BIO_METHOD* NonBlockingFilterBIO::method()
{
	/// Created once and never freed, like the methods of OpenSSL.
	static BIO_METHOD* const created = []
	{
		const int index = BIO_get_new_index();
		if (index == -1)
			throw SSLException("Cannot get a BIO type index");
		BIO_METHOD* result = BIO_meth_new(index | BIO_TYPE_FILTER, "non-blocking filter");
		if (!result)
			throw SSLException("Cannot create BIO method");
		BIO_meth_set_write(result, write);
		BIO_meth_set_read(result, read);
		BIO_meth_set_ctrl(result, ctrl);
		BIO_meth_set_destroy(result, destroy);
		return result;
	}();
	return created;
}


int NonBlockingFilterBIO::write(BIO* bio, const char* data, int length)
{
	auto& filter = *static_cast<NonBlockingFilterBIO*>(BIO_get_data(bio));
	BIO_clear_retry_flags(bio);
	if (filter.nonBlocking)
		return filter.writeNonBlocking(data, length);

	const int queued = filter.writeQueuedToNext(bio);
	if (queued <= 0)
		return queued;
	const int rc = BIO_write(BIO_next(bio), data, length);
	BIO_copy_next_retry(bio);
	return rc;
}


int NonBlockingFilterBIO::read(BIO* bio, char* data, int length)
{
	auto& filter = *static_cast<NonBlockingFilterBIO*>(BIO_get_data(bio));
	BIO_clear_retry_flags(bio);
	if (filter.nonBlocking)
		return filter.readNonBlocking(bio, data, length);

	/// A failed write of the queued bytes keeps its retry flags: `SSL_read` then reports
	/// `SSL_ERROR_WANT_WRITE`, and the blocking caller waits until the socket can send.
	const int queued = filter.writeQueuedToNext(bio);
	if (queued <= 0)
		return queued;
	const int rc = BIO_read(BIO_next(bio), data, length);
	BIO_copy_next_retry(bio);
	return rc;
}


long NonBlockingFilterBIO::ctrl(BIO* bio, int cmd, long num, void* ptr) // NOLINT(google-runtime-int): the signature of OpenSSL
{
	auto& filter = *static_cast<NonBlockingFilterBIO*>(BIO_get_data(bio));
	BIO* next = BIO_next(bio);
	switch (cmd)
	{
	case BIO_CTRL_WPENDING:
		return static_cast<long>(filter.queue.size() - filter.queueOffset) + (next ? BIO_ctrl(next, cmd, num, ptr) : 0); // NOLINT(google-runtime-int)
	case BIO_CTRL_FLUSH:
		BIO_clear_retry_flags(bio);
		if (filter.nonBlocking)
		{
			/// Never waits: what is left goes out with the next operation.
			try
			{
				filter.sendQueued();
			}
			catch (...)
			{
				filter.caught = std::current_exception();
				return 0;
			}
			return 1;
		}
		else
		{
			const int queued = filter.writeQueuedToNext(bio);
			if (queued <= 0)
				return queued;
		}
		break;
#if defined(BIO_CTRL_GET_KTLS_SEND)
	case BIO_CTRL_GET_KTLS_SEND:
	case BIO_CTRL_GET_KTLS_RECV:
		/// The kernel must not encrypt the records: the queued bytes are already encrypted.
		return 0;
#endif
	default:
		break;
	}
	return next ? BIO_ctrl(next, cmd, num, ptr) : 0;
}


int NonBlockingFilterBIO::destroy(BIO* bio)
{
	delete static_cast<NonBlockingFilterBIO*>(BIO_get_data(bio));
	BIO_set_data(bio, nullptr);
	BIO_set_init(bio, 0);
	return 1;
}


int NonBlockingFilterBIO::writeNonBlocking(const char* data, int length)
{
	try
	{
		int rest = length;
		if (sendQueued())
		{
			const int sent = socket.sendNow(data, length);
			/// The socket holds the error: the connection cannot send any more, and the bytes are dropped.
			if (sent < 0)
				return length;
			data += sent;
			rest -= sent;
		}
		if (rest > 0)
			queueBytes(data, static_cast<size_t>(rest));
		return length;
	}
	catch (...)
	{
		caught = std::current_exception();
		return -1;
	}
}


int NonBlockingFilterBIO::readNonBlocking(BIO* bio, char* data, int length)
{
	try
	{
		/// The peer can wait for the queued bytes before it answers.
		sendQueued();
		const int received = socket.receiveNow(data, length);
		if (received > 0)
			return received;
		if (received == 0)
		{
			BIO_set_retry_read(bio);
			return -1;
		}
		/// The end of the stream.
		return 0;
	}
	catch (...)
	{
		caught = std::current_exception();
		return -1;
	}
}


int NonBlockingFilterBIO::writeQueuedToNext(BIO* bio)
{
	while (hasQueued())
	{
		const size_t left = queue.size() - queueOffset;
		const int rc = BIO_write(BIO_next(bio), queue.data() + queueOffset, static_cast<int>(std::min<size_t>(left, INT_MAX)));
		if (rc <= 0)
		{
			BIO_copy_next_retry(bio);
			return -1;
		}
		queueOffset += static_cast<size_t>(rc);
	}
	dropQueue();
	return 1;
}


void NonBlockingFilterBIO::queueBytes(const char* data, size_t length)
{
	if (queueOffset > 0)
	{
		queue.erase(0, queueOffset);
		queueOffset = 0;
	}
	queue.append(data, length);
}


void NonBlockingFilterBIO::dropQueue()
{
	queue.clear();
	queueOffset = 0;
}


} } // namespace Poco::Net
