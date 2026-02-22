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
#include "Poco/Net/X509Certificate.h"
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

SecureSocketImpl::SecureSocketImpl(Poco::AutoPtr<SocketImpl> pSocketImpl, Context::Ptr pContext):
	_pSSL(nullptr),
	_pSocket(pSocketImpl),
	_pContext(pContext),
	_needHandshake(false)
{
	poco_check_ptr (_pSocket);
	poco_check_ptr (_pContext);
}


SecureSocketImpl::~SecureSocketImpl()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
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
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_assert (!_pSSL);

	StreamSocket ss = _pSocket->acceptConnection(clientAddr);
	Poco::AutoPtr<SecureStreamSocketImpl> pSecureStreamSocketImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(ss.impl()), _pContext);
	pSecureStreamSocketImpl->acceptSSL();
	pSecureStreamSocketImpl->duplicate();
	return pSecureStreamSocketImpl;
}


void SecureSocketImpl::acceptSSL()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_assert (!_pSSL);

	BIO* pBIO = BIO_new(BIO_s_socket());
	if (!pBIO) throw SSLException("Cannot create BIO object");
	BIO_set_fd(pBIO, static_cast<int>(_pSocket->sockfd()), BIO_NOCLOSE);

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
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connect(address);
	connectSSL(performHandshake);
}


void SecureSocketImpl::connect(const SocketAddress& address, const Poco::Timespan& timeout, bool performHandshake)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
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
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	if (_pSSL) reset();

	poco_assert (!_pSSL);

	_pSocket->connectNB(address);
	connectSSL(false);
}


void SecureSocketImpl::connectSSL(bool performHandshake)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_assert (!_pSSL);
	poco_assert (_pSocket->initialized());

	BIO* pBIO = BIO_new(BIO_s_socket());
	if (!pBIO) throw SSLException("Cannot create SSL BIO object");
	BIO_set_fd(pBIO, static_cast<int>(_pSocket->sockfd()), BIO_NOCLOSE);

	_pSSL = SSL_new(_pContext->sslContext());
	if (!_pSSL)
	{
		BIO_free(pBIO);
		throw SSLException("Cannot create SSL object");
	}
	SSL_set_bio(_pSSL, pBIO, pBIO);

#if OPENSSL_VERSION_NUMBER >= 0x0908060L && !defined(OPENSSL_NO_TLSEXT)
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
			int ret;
			Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
			do
			{
				RemainingTimeCounter counter(remaining_time);
				ret = SSL_connect(_pSSL);
			}
			while (mustRetry(ret, remaining_time));
			handleError(ret);
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
		throw;
	}
}


void SecureSocketImpl::bind(const SocketAddress& address, bool reuseAddress, bool reusePort)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_check_ptr (_pSocket);

	_pSocket->bind(address, reuseAddress, reusePort);
}


void SecureSocketImpl::listen(int backlog)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_check_ptr (_pSocket);

	_pSocket->listen(backlog);
}


void SecureSocketImpl::shutdown()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	if (_pSSL)
	{
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
			int rc = SSL_shutdown(_pSSL);
			if (rc < 0) handleError(rc);
			if (_pSocket->getBlocking())
			{
				_pSocket->shutdown();
			}
		}
	}
}


void SecureSocketImpl::close()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
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
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

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

	Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
	do
	{
		RemainingTimeCounter counter(remaining_time);
		rc = SSL_write(_pSSL, buffer, length);
	}
	while (mustRetry(rc, remaining_time));
	if (rc <= 0)
	{
		// At this stage we still can have last not yet received SSL message containing SSL error
		// so make a read to force SSL to process possible SSL error
		if (SSL_get_error(_pSSL, rc) == SSL_ERROR_SYSCALL && SocketImpl::lastError() == POCO_ECONNRESET)
		{
			char c = 0;
			SSL_read(_pSSL, &c, 1);
		}

		rc = handleError(rc);
		if (rc == 0) throw SSLConnectionUnexpectedlyClosedException();
	}
	return rc;
}


int SecureSocketImpl::receiveBytes(void* buffer, int length, int flags)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	/// Special case: just check that we can read from socket
	if ((flags & MSG_DONTWAIT) && (flags & MSG_PEEK))
		return _pSocket->receiveBytes(buffer, length, flags);

	int rc;
	if (_needHandshake)
	{
		rc = completeHandshake();
		if (rc == 1)
			verifyPeerCertificate();
		else
			return rc;
	}

	Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
	do
	{
		/// SSL record may consist of several TCP packets,
		/// so thread can be blocked on recv/send and epoll_wait several times
		/// until SSL_read will return rc > 0. Let's use our own time counter.
		RemainingTimeCounter counter(remaining_time);
		rc = SSL_read(_pSSL, buffer, length);
	}
	while (mustRetry(rc, remaining_time));
	if (rc <= 0)
	{
		return handleError(rc);
	}
	return rc;
}


int SecureSocketImpl::available() const
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_check_ptr (_pSSL);

	return SSL_pending(_pSSL);
}


int SecureSocketImpl::completeHandshake()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	poco_assert (_pSocket->initialized());
	poco_check_ptr (_pSSL);

	int rc;
	Poco::Timespan remaining_time = getMaxTimeoutOrLimit();
	do
	{
		RemainingTimeCounter counter(remaining_time);
		rc = SSL_do_handshake(_pSSL);
	}
	while (mustRetry(rc, remaining_time));
	if (rc <= 0)
	{
		return handleError(rc);
	}
	_needHandshake = false;
	return rc;
}


void SecureSocketImpl::verifyPeerCertificate()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	if (_peerHostName.empty())
		verifyPeerCertificate(_pSocket->peerAddress().host().toString());
	else
		verifyPeerCertificate(_peerHostName);
}


void SecureSocketImpl::verifyPeerCertificate(const std::string& hostName)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	long certErr = verifyPeerCertificateImpl(hostName);
	if (certErr != X509_V_OK)
	{
		std::string msg = Utility::convertCertificateError(certErr);
		throw CertificateValidationException("Unacceptable certificate from " + hostName, msg);
	}
}


long SecureSocketImpl::verifyPeerCertificateImpl(const std::string& hostName)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	Context::VerificationMode mode = _pContext->verificationMode();
	if (mode == Context::VERIFY_NONE || !_pContext->extendedCertificateVerificationEnabled() ||
	    (mode != Context::VERIFY_STRICT && isLocalHost(hostName)))
	{
		return X509_V_OK;
	}

	X509* pCert = SSL_get_peer_certificate(_pSSL);
	if (pCert)
	{
		X509Certificate cert(pCert);
		return cert.verify(hostName) ? X509_V_OK : X509_V_ERR_APPLICATION_VERIFICATION;
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
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	if (_pSSL)
		return SSL_get_peer_certificate(_pSSL);
	else
		return 0;
}

Poco::Timespan SecureSocketImpl::getMaxTimeoutOrLimit()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
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

bool SecureSocketImpl::mustRetry(int rc, Poco::Timespan& remaining_time)
{
	if (remaining_time == 0)
		return false;
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	if (rc <= 0)
	{
		int sslError = SSL_get_error(_pSSL, rc);
		int socketError = _pSocket->lastError();
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
			return socketError == POCO_EINTR;
		}
	}
	return false;
}


int SecureSocketImpl::handleError(int rc)
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	if (rc > 0) return rc;

	int sslError = SSL_get_error(_pSSL, rc);
	int error = SocketImpl::lastError();

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
		poco_bugcheck();
		return rc;
	case SSL_ERROR_SYSCALL:
		if (error != 0)
		{
			SocketImpl::error(error);
		}
		// fallthrough
	default:
		{
			long lastError = ERR_get_error();
			if (lastError == 0)
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
				ERR_error_string_n(lastError, buffer, sizeof(buffer));
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
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	_peerHostName = peerHostName;
}


void SecureSocketImpl::reset()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	close();
	if (_pSSL)
	{
		SSL_free(_pSSL);
		_pSSL = nullptr;
	}
}


void SecureSocketImpl::abort()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	_pSocket->shutdown();
}


Session::Ptr SecureSocketImpl::currentSession()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
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
	std::lock_guard<std::recursive_mutex> lock(_mutex);
	_pSession = pSession;
}


bool SecureSocketImpl::sessionWasReused()
{
	std::lock_guard<std::recursive_mutex> lock(_mutex);
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
