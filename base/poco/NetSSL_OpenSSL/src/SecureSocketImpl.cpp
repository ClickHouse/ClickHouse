//
// SecureSocketImpl.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureSocketImpl
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SecureSocketImpl.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/Utility.h"
#include "Poco/Net/SecureStreamSocket.h"
#include "Poco/Net/SecureStreamSocketImpl.h"
#include "Poco/Net/StreamSocketImpl.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/DNS.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/Format.h"
#include <cerrno>
#include <openssl/x509v3.h>
#include <openssl/err.h>


using Poco::IOException;
using Poco::TimeoutException;
using Poco::InvalidArgumentException;
using Poco::NumberFormatter;
using Poco::Timespan;


// workaround for C++-incompatible macro
#define POCO_BIO_set_nbio_accept(b,n) BIO_ctrl(b,BIO_C_SET_ACCEPT,1,(void*)((n)?"a":NULL))


namespace Poco {
namespace Net {

struct RemainingTimeCounter
{
	explicit RemainingTimeCounter(Poco::Timespan& remainingTime_) : remainingTime(remainingTime_) {};
	~RemainingTimeCounter()
	{
		Poco::Timestamp end;
		Poco::Timespan waited = end - start;
		if (waited < remainingTime)
			remainingTime -= waited;
		else
			remainingTime = 0;
	}
private:
	Poco::Timespan& remainingTime;
	Poco::Timestamp start;
};


struct SSLOperationResult
{
	int rc = 0;
	int sslError = SSL_ERROR_NONE;
	int socketError = 0;
	unsigned long errorCode = 0;
};


template <typename Operation>
SSLOperationResult performSSLOperation(SSL * ssl, Operation && operation, bool zeroIsError = true)
{
	/// The error queue contract for TLS I/O and `SSL_get_error`:
	/// https://docs.openssl.org/3.5/man3/SSL_get_error/
	/// The queue is cleared with `ERR_clear_error`:
	/// https://docs.openssl.org/3.5/man3/ERR_clear_error/
	ERR_clear_error();

	SSLOperationResult result;
	result.rc = operation();

	if (result.rc < 0 || (zeroIsError && result.rc == 0))
	{
		/// Save `errno` before calling another function. `SSL_get_error` must be the first
		/// OpenSSL call after the operation and must observe the queue created by that operation.
		result.socketError = errno;
		result.sslError = SSL_get_error(ssl, result.rc);
		result.errorCode = ERR_get_error();
	}

	/// `errorCode` above preserves the diagnostic used by `handleError`. Do not leave any
	/// additional entries in the thread-local queue for another connection on this thread.
	ERR_clear_error();
	return result;
}

SecureSocketImpl::SecureSocketImpl(Poco::AutoPtr<SocketImpl> pSocketImpl, Context::Ptr pContext):
	_pSSL(nullptr),
	_pSocket(pSocketImpl),
	_pContext(pContext),
	_needHandshake(false),
	_fatalError(false)
{
	poco_check_ptr (_pSocket);
	poco_check_ptr (_pContext);
}


SecureSocketImpl::~SecureSocketImpl()
{
	ScopedLock lock(*_mutex);
	try
	{
		reset();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


SocketImpl* SecureSocketImpl::acceptConnection(SocketAddress& clientAddr)
{
	ScopedLock lock(*_mutex);
	poco_assert (!_pSSL);

	StreamSocket ss = _pSocket->acceptConnection(clientAddr);
	Poco::AutoPtr<SecureStreamSocketImpl> pSecureStreamSocketImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(ss.impl()), _pContext);
	pSecureStreamSocketImpl->acceptSSL();
	pSecureStreamSocketImpl->duplicate();
	return pSecureStreamSocketImpl;
}


void SecureSocketImpl::setBioMethod(const BIO_METHOD * method)
{
	_bioMethod = method;
}


void SecureSocketImpl::setMutex(std::unique_ptr<RecursiveMutex> mutex)
{
	poco_check_ptr (mutex);
	_mutex = std::move(mutex);
}


void SecureSocketImpl::markFatalError()
{
	ScopedLock lock(*_mutex);
	_fatalError = true;
}


const BIO_METHOD * SecureSocketImpl::getBioMethod() const
{
	return _bioMethod ? _bioMethod : BIO_s_socket();
}


void SecureSocketImpl::acceptSSL()
{
	ScopedLock lock(*_mutex);
	poco_assert (!_pSSL);
	_fatalError = false;

	BIO* pBIO = BIO_new(getBioMethod());
	if (!pBIO) throw SSLException("Cannot create BIO object");
	BIO_set_fd(pBIO, static_cast<int>(_pSocket->sockfd()), BIO_NOCLOSE);

	if (_bioMethod)
		BIO_set_data(pBIO, _pSocket.get());

	_pSSL = SSL_new(_pContext->sslContext());
	if (!_pSSL)
	{
		BIO_free(pBIO);
		throw SSLException("Cannot create SSL object");
	}
	SSL_set_bio(_pSSL, pBIO, pBIO);
	SSL_set_accept_state(_pSSL);
	_needHandshake = true;
}


void SecureSocketImpl::connect(const SocketAddress& address, bool performHandshake)
{
	ScopedLock lock(*_mutex);
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connect(address);
	connectSSL(performHandshake);
}


void SecureSocketImpl::connect(const SocketAddress& address, const Poco::Timespan& timeout, bool performHandshake)
{
	ScopedLock lock(*_mutex);
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connect(address, timeout);
	//FIXME it updates timeouts of SecureStreamSocketImpl::underlying_socket it does not update timeouts of SecureStreamSocketImpl
	//However, timeouts of SecureStreamSocketImpl are not used in connectSSL() and previous settings are restored after
	Poco::Timespan receiveTimeout = _pSocket->getReceiveTimeout();
	Poco::Timespan sendTimeout = _pSocket->getSendTimeout();
	_pSocket->setReceiveTimeout(timeout);
	_pSocket->setSendTimeout(timeout);
	connectSSL(performHandshake);
	_pSocket->setReceiveTimeout(receiveTimeout);
	_pSocket->setSendTimeout(sendTimeout);
}


void SecureSocketImpl::connectNB(const SocketAddress& address)
{
	ScopedLock lock(*_mutex);
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connectNB(address);
	connectSSL(false);
}


void SecureSocketImpl::connectSSL(bool performHandshake)
{
	ScopedLock lock(*_mutex);
	poco_assert (!_pSSL);
	poco_assert (_pSocket->initialized());
	_fatalError = false;

	BIO* pBIO = BIO_new(getBioMethod());
	if (!pBIO) throw SSLException("Cannot create SSL BIO object");
	BIO_set_fd(pBIO, static_cast<int>(_pSocket->sockfd()), BIO_NOCLOSE);

	if (_bioMethod)
		BIO_set_data(pBIO, _pSocket.get());

	_pSSL = SSL_new(_pContext->sslContext());
	if (!_pSSL)
	{
		BIO_free(pBIO);
		throw SSLException("Cannot create SSL object");
	}
	SSL_set_bio(_pSSL, pBIO, pBIO);

#if !defined(OPENSSL_NO_TLSEXT)
	if (!_peerHostName.empty())
	{
		SSL_set_tlsext_host_name(_pSSL, _peerHostName.c_str());
	}
#endif

	if (_pSession)
	{
		SSL_set_session(_pSSL, _pSession->sslSession());
	}

	try
	{
		if (performHandshake && _pSocket->getBlocking())
		{
			SSLOperationResult result;
			Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
			do
			{
				RemainingTimeCounter counter(remaining_time);
				result = performSSLOperation(_pSSL, [this]
				{
					return SSL_connect(_pSSL);
				});
			}
			while (mustRetry(result.rc, result.sslError, result.socketError, remaining_time));
			handleError(result.rc, result.sslError, result.socketError, result.errorCode);
			verifyPeerCertificate();
		}
		else
		{
			SSL_set_connect_state(_pSSL);
			_needHandshake = true;
		}
	}
	catch (...)
	{
		SSL_free(_pSSL);
		_pSSL = 0;
		_fatalError = false;
		throw;
	}
}


void SecureSocketImpl::bind(const SocketAddress& address, bool reuseAddress, bool reusePort)
{
	ScopedLock lock(*_mutex);
	poco_check_ptr (_pSocket);

	_pSocket->bind(address, reuseAddress, reusePort);
}


void SecureSocketImpl::listen(int backlog)
{
	ScopedLock lock(*_mutex);
	poco_check_ptr (_pSocket);

	_pSocket->listen(backlog);
}


void SecureSocketImpl::shutdown()
{
	ScopedLock lock(*_mutex);
	if (_pSSL)
	{
		if (_fatalError)
		{
			/// OpenSSL forbids `SSL_shutdown` after a fatal TLS or syscall error.
			/// https://docs.openssl.org/3.5/man3/SSL_shutdown/
			/// Close the underlying transport without attempting an orderly TLS shutdown.
			if (_pSocket->getBlocking())
				_pSocket->shutdown();
			return;
		}

        // Don't shut down the socket more than once.
        int shutdownState = SSL_get_shutdown(_pSSL);
        bool shutdownSent = (shutdownState & SSL_SENT_SHUTDOWN) == SSL_SENT_SHUTDOWN;
        if (!shutdownSent)
        {
			// A proper clean shutdown would require us to
			// retry the shutdown if we get a zero return
			// value, until SSL_shutdown() returns 1.
			// However, this will lead to problems with
			// most web browsers, so we just set the shutdown
			// flag by calling SSL_shutdown() once and be
			// done with it.
			/// A zero result is not an error for `SSL_shutdown` and must not be passed to
			/// `SSL_get_error`; it means that `close_notify` was sent but not received yet.
			SSLOperationResult result;
			Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
			do
			{
				RemainingTimeCounter counter(remaining_time);
				result = performSSLOperation(_pSSL, [this]
				{
					return SSL_shutdown(_pSSL);
				}, false);
			}
			while (result.rc < 0 && mustRetry(result.rc, result.sslError, result.socketError, remaining_time));
			if (result.rc < 0)
				handleError(result.rc, result.sslError, result.socketError, result.errorCode);
			if (_pSocket->getBlocking())
			{
				_pSocket->shutdown();
			}
		}
	}
}


void SecureSocketImpl::close()
{
	ScopedLock lock(*_mutex);
	try
	{
		shutdown();
	}
	catch (...)
	{
	}
	_pSocket->close();
}


int SecureSocketImpl::sendBytes(const void* buffer, int length, int flags)
{
	ScopedLock lock(*_mutex);
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	_pSocket->throttleSend(length, _pSocket->getBlocking() && (flags & MSG_DONTWAIT) == 0);

	int rc;
	if (_needHandshake)
	{
		rc = completeHandshake();
		if (rc == 1)
			verifyPeerCertificate();
		else if (rc == 0)
			throw SSLConnectionUnexpectedlyClosedException();
		else
			return rc;
	}

	SSLOperationResult result;
	Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
	do
	{
		RemainingTimeCounter counter(remaining_time);
		result = performSSLOperation(_pSSL, [this, buffer, length]
		{
			return SSL_write(_pSSL, buffer, length);
		});
	}
	while (mustRetry(result.rc, result.sslError, result.socketError, remaining_time));
	rc = result.rc;
	if (rc <= 0)
	{
		rc = handleError(rc, result.sslError, result.socketError, result.errorCode);
		if (rc == 0) throw SSLConnectionUnexpectedlyClosedException();
		if (rc < 0 && _pSocket->getBlocking())
			throw Poco::TimeoutException("SSL_write timed out");
	}

	_pSocket->useSendThrottlerBudget(rc);

	return rc;
}


int SecureSocketImpl::receiveBytes(void* buffer, int length, int flags)
{
	ScopedLock lock(*_mutex);
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	/// Special case: just check that we can read from socket
	if ((flags & MSG_DONTWAIT) && (flags & MSG_PEEK))
		return _pSocket->receiveBytes(buffer, length, flags);

	_pSocket->throttleRecv(length, _pSocket->getBlocking() && (flags & MSG_DONTWAIT) == 0);

	int rc;
	if (_needHandshake)
	{
		rc = completeHandshake();
		if (rc == 1)
			verifyPeerCertificate();
		else
			return rc;
	}

	SSLOperationResult result;
	Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
	do
	{
		/// SSL record may consist of several TCP packets,
		/// so thread can be blocked on recv/send and epoll_wait several times
		/// until SSL_read will return rc > 0. Let's use our own time counter.
		RemainingTimeCounter counter(remaining_time);
		result = performSSLOperation(_pSSL, [this, buffer, length]
		{
			return SSL_read(_pSSL, buffer, length);
		});
	}
	while (mustRetry(result.rc, result.sslError, result.socketError, remaining_time));
	rc = result.rc;
	if (rc <= 0)
	{
		rc = handleError(rc, result.sslError, result.socketError, result.errorCode);
		if (rc < 0 && _pSocket->getBlocking())
			throw Poco::TimeoutException("SSL_read timed out");
		return rc;
	}

	_pSocket->useRecvThrottlerBudget(rc);

	return rc;
}


int SecureSocketImpl::available() const
{
	ScopedLock lock(*_mutex);
	poco_check_ptr (_pSSL);

	return SSL_pending(_pSSL);
}


int SecureSocketImpl::completeHandshake()
{
	ScopedLock lock(*_mutex);
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	SSLOperationResult result;
	Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
	do
	{
		RemainingTimeCounter counter(remaining_time);
		result = performSSLOperation(_pSSL, [this]
		{
			return SSL_do_handshake(_pSSL);
		});
	}
	while (mustRetry(result.rc, result.sslError, result.socketError, remaining_time));
	int rc = result.rc;
	if (rc <= 0)
	{
		rc = handleError(rc, result.sslError, result.socketError, result.errorCode);
		if (rc < 0 && _pSocket->getBlocking())
			throw Poco::TimeoutException("SSL handshake timed out");
		return rc;
	}
	_needHandshake = false;
	return rc;
}


void SecureSocketImpl::verifyPeerCertificate()
{
	ScopedLock lock(*_mutex);
	if (_peerHostName.empty())
		verifyPeerCertificate(_pSocket->peerAddress().host().toString());
	else
		verifyPeerCertificate(_peerHostName);
}


void SecureSocketImpl::verifyPeerCertificate(const std::string& hostName)
{
	ScopedLock lock(*_mutex);
	long certErr = verifyPeerCertificateImpl(hostName);
	if (certErr != X509_V_OK)
	{
		std::string msg = Utility::convertCertificateError(certErr);
		throw CertificateValidationException("Unacceptable certificate from " + hostName, msg);
	}
}


long SecureSocketImpl::verifyPeerCertificateImpl(const std::string& hostName)
{
	ScopedLock lock(*_mutex);
	Context::VerificationMode mode = _pContext->verificationMode();
	if (mode == Context::VERIFY_NONE || !_pContext->extendedCertificateVerificationEnabled() ||
	    (mode != Context::VERIFY_STRICT && isLocalHost(hostName)))
		return X509_V_OK;

	// SSL_get1_peer_certificate returns a certificate whose reference count has
	// been incremented; the caller owns that reference and must X509_free it,
	// otherwise the peer certificate leaks on every verified handshake.
	X509* pCert = SSL_get1_peer_certificate(_pSSL);
	if (pCert)
	{
        long result = X509_V_ERR_APPLICATION_VERIFICATION;
        if (X509_check_host(pCert, hostName.c_str(), hostName.length(), 0, nullptr) == 1)
        {
            result = X509_V_OK;
        }
        else
        {
            IPAddress ip;
            if (IPAddress::tryParse(hostName, ip))
            {
                result = X509_check_ip_asc(pCert, hostName.c_str(), 0) == 1 ? X509_V_OK : X509_V_ERR_APPLICATION_VERIFICATION;
            }
        }
        X509_free(pCert);
        return result;
	}
	else return X509_V_OK;
}

/// This is static method, that's why no lock
bool SecureSocketImpl::isLocalHost(const std::string& hostName)
{
	try
	{
		SocketAddress addr(hostName, 0);
		return addr.host().isLoopback();
	}
	catch (Poco::Exception&)
	{
		return false;
	}
}


X509* SecureSocketImpl::peerCertificate() const
{
	ScopedLock lock(*_mutex);
	if (_pSSL)
		return SSL_get1_peer_certificate(_pSSL);
	else
		return 0;
}

Poco::Timespan SecureSocketImpl::getMaxTimeoutOrLimit()
{
	ScopedLock lock(*_mutex);
	Poco::Timespan remaining_time = _pSocket->getReceiveTimeout();
	Poco::Timespan send_timeout = _pSocket->getSendTimeout();
	if (remaining_time < send_timeout)
		remaining_time = send_timeout;
	/// zero SO_SNDTIMEO/SO_RCVTIMEO works as no timeout, let's replicate this
	///
	/// NOTE: we cannot use INT64_MAX (std::numeric_limits<Poco::Timespan::TimeDiff>::max()),
	/// since it will be later passed to poll() which accept int timeout, and
	/// even though poll() accepts milliseconds and Timespan() accepts
	/// microseconds, let's use smaller maximum value just to avoid some possible
	/// issues, this should be enough anyway (it is ~24 days).
	if (remaining_time == 0)
		remaining_time = Poco::Timespan(std::numeric_limits<int>::max());
	return remaining_time;
}

bool SecureSocketImpl::mustRetry(int rc, int sslError, int socketError, Poco::Timespan& remaining_time)
{
	if (remaining_time == 0)
		return false;
	ScopedLock lock(*_mutex);
	if (rc <= 0)
	{
		switch (sslError)
		{
		case SSL_ERROR_WANT_READ:
			if (_pSocket->getBlocking())
			{
				if (_pSocket->pollImpl(remaining_time, Poco::Net::Socket::SELECT_READ))
					return true;
				else
					throw Poco::TimeoutException();
			}
			break;
		case SSL_ERROR_WANT_WRITE:
			if (_pSocket->getBlocking())
			{
				if (_pSocket->pollImpl(remaining_time, Poco::Net::Socket::SELECT_WRITE))
					return true;
				else
					throw Poco::TimeoutException();
			}
			break;
		/// NOTE: POCO_EINTR is the same as SSL_ERROR_WANT_READ (at least in
		/// OpenSSL), so this likely dead code, but let's leave it for
		/// compatibility with other implementations
		case SSL_ERROR_SYSCALL:
			return socketError == POCO_EAGAIN || socketError == POCO_EINTR;
		default:
			/// `errno` is only meaningful for `SSL_ERROR_SYSCALL`; other errors must not
			/// be retried because of a leftover `EINTR`, especially `SSL_ERROR_SSL`.
			return false;
		}
	}
	return false;
}


int SecureSocketImpl::handleError(int rc, int sslError, int error, unsigned long errorCode)
{
	ScopedLock lock(*_mutex);
	if (rc > 0) return rc;

	switch (sslError)
	{
	case SSL_ERROR_ZERO_RETURN:
		return 0;
	case SSL_ERROR_WANT_READ:
		return SecureStreamSocket::ERR_SSL_WANT_READ;
	case SSL_ERROR_WANT_WRITE:
		return SecureStreamSocket::ERR_SSL_WANT_WRITE;
	case SSL_ERROR_WANT_CONNECT:
	case SSL_ERROR_WANT_ACCEPT:
	case SSL_ERROR_WANT_X509_LOOKUP:
		// these should not occur
		_fatalError = true;
		poco_bugcheck();
		return rc;
	case SSL_ERROR_SYSCALL:
		_fatalError = true;
		if (error != 0)
		{
			SocketImpl::error(error);
		}
		[[fallthrough]];
	case SSL_ERROR_SSL:
	default:
		{
			_fatalError = true;
			if (errorCode == 0)
			{
				if (rc == 0)
				{
					// Most web browsers do this, don't report an error
					if (_pContext->isForServerUse())
						return 0;
					else
						throw SSLConnectionUnexpectedlyClosedException();
				}
				else if (rc == -1)
				{
					throw SSLConnectionUnexpectedlyClosedException();
				}
				else
				{
					SecureStreamSocketImpl::error(Poco::format("The BIO reported an error: %d", rc));
				}
			}
			else
			{
				char buffer[256];
				ERR_error_string_n(errorCode, buffer, sizeof(buffer));
				std::string msg(buffer);
				throw SSLException(msg);
			}
		}
 		break;
	}
	return rc;
}


void SecureSocketImpl::setPeerHostName(const std::string& peerHostName)
{
	ScopedLock lock(*_mutex);
	_peerHostName = peerHostName;
}


void SecureSocketImpl::reset()
{
	ScopedLock lock(*_mutex);
	close();
	if (_pSSL)
	{
		SSL_free(_pSSL);
		_pSSL = nullptr;
	}
	_fatalError = false;
}


void SecureSocketImpl::abort()
{
	ScopedLock lock(*_mutex);
	_pSocket->shutdown();
}


Session::Ptr SecureSocketImpl::currentSession()
{
	ScopedLock lock(*_mutex);
	if (_pSSL)
	{
		SSL_SESSION* pSession = SSL_get1_session(_pSSL);
		if (pSession)
		{
			if (_pSession && pSession == _pSession->sslSession())
			{
				SSL_SESSION_free(pSession);
				return _pSession;
			}
			else return new Session(pSession);
		}
	}
	return 0;
}


void SecureSocketImpl::useSession(Session::Ptr pSession)
{
	ScopedLock lock(*_mutex);
	_pSession = pSession;
}


bool SecureSocketImpl::sessionWasReused()
{
	ScopedLock lock(*_mutex);
	if (_pSSL)
		return SSL_session_reused(_pSSL) != 0;
	else
		return false;
}

void SecureSocketImpl::setBlocking(bool flag)
{
    _pSocket->setBlocking(flag);
}

bool SecureSocketImpl::getBlocking() const
{
    return _pSocket->getBlocking();
}


} } // namespace Poco::Net
