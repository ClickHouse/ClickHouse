//
// SecureSocketImpl.cpp
//
// Library: NetSSL_Win
// Package: SSLSockets
// Module:  SecureSocketImpl
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SecureSocketImpl.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/Utility.h"
#include "Poco/Net/SecureStreamSocketImpl.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/StreamSocketImpl.h"
#include "Poco/Format.h"
#include "Poco/UnicodeConverter.h"
#include <wininet.h>
#include <cstring>


namespace Poco {
namespace Net {


class StateMachine
{
public:
	typedef bool (StateMachine::*ConditionMethod)(SOCKET sockfd);
	typedef void (SecureSocketImpl::*StateImpl)(void);

	StateMachine();
	~StateMachine();

	static StateMachine& instance();

	// Conditions
	bool readable(SOCKET sockfd);
	bool writable(SOCKET sockfd);
	bool readOrWritable(SOCKET sockfd);
	bool none(SOCKET sockfd);
	void select(fd_set* fdRead, fd_set* fdWrite, SOCKET sockfd);

	void execute(SecureSocketImpl* pSock);

private:
	StateMachine(const StateMachine&);
	StateMachine& operator = (const StateMachine&);

	typedef std::pair<ConditionMethod, StateImpl> ConditionState;
	std::vector<ConditionState> _states;
};


SecureSocketImpl::SecureSocketImpl(Poco::AutoPtr<SocketImpl> pSocketImpl, Context::Ptr pContext):
	_pSocket(pSocketImpl),
	_pContext(pContext),
	_mode(pContext->isForServerUse() ? MODE_SERVER : MODE_CLIENT),
	_clientAuthRequired(pContext->verificationMode() >= Context::VERIFY_STRICT),
	_securityFunctions(SSLManager::instance().securityFunctions()),
	_pOwnCertificate(0),
	_pPeerCertificate(0),
	_hCreds(),
	_hContext(),
	_contextFlags(0),
	_overflowBuffer(0),
	_sendBuffer(0),
	_recvBuffer(IO_BUFFER_SIZE),
	_recvBufferOffset(0),
	_ioBufferSize(0),
	_streamSizes(),
	_outSecBuffer(&_securityFunctions, true),
	_inSecBuffer(&_securityFunctions, false),
	_extraSecBuffer(),
	_securityStatus(SEC_E_INCOMPLETE_MESSAGE),
	_state(ST_INITIAL),
	_needData(true),
	_needHandshake(false)
{
	_hCreds.dwLower = 0;
	_hCreds.dwUpper = 0;

	_hContext.dwLower = 0;
	_hContext.dwUpper = 0;

	_streamSizes.cbBlockSize      = 0;
	_streamSizes.cbHeader         = 0;
	_streamSizes.cbMaximumMessage = 0;
	_streamSizes.cbTrailer        = 0;

	_overflowBuffer.resize(0);

	initCommon();
}


SecureSocketImpl::~SecureSocketImpl()
{
	cleanup();
}


void SecureSocketImpl::initCommon()
{
	_contextFlags = ISC_REQ_SEQUENCE_DETECT
	              | ISC_REQ_REPLAY_DETECT
	              | ISC_REQ_CONFIDENTIALITY
	              | ISC_RET_EXTENDED_ERROR
	              | ISC_REQ_ALLOCATE_MEMORY
	              | ISC_REQ_STREAM;

	if (_pContext->verificationMode() == Context::VERIFY_NONE)
	{
		_contextFlags |= ISC_REQ_MANUAL_CRED_VALIDATION;
	}
	else if (_pContext->verificationMode() == Context::VERIFY_RELAXED)
	{
		_contextFlags |= ISC_REQ_INTEGRITY;
	}
	else if (_pContext->verificationMode() >= Context::VERIFY_STRICT)
	{
		_contextFlags |= ISC_REQ_INTEGRITY;
	}

	if (_pContext->verificationMode() == Context::VERIFY_RELAXED)
	{
		_contextFlags |= ISC_REQ_MANUAL_CRED_VALIDATION;
	}
}


void SecureSocketImpl::cleanup()
{
	_peerHostName.clear();

	_hCreds.dwLower = 0;
	_hCreds.dwUpper = 0;

	if (_hContext.dwLower != 0 && _hContext.dwUpper != 0)
	{
		_securityFunctions.DeleteSecurityContext(&_hContext);
		_hContext.dwLower = 0;
		_hContext.dwUpper = 0;
	}

	if (_pOwnCertificate)
	{
		CertFreeCertificateContext(_pOwnCertificate);
		_pOwnCertificate = 0;
	}

	if (_pPeerCertificate)
	{
		CertFreeCertificateContext(_pPeerCertificate);
		_pPeerCertificate = 0;
	}

	_outSecBuffer.release();
	_inSecBuffer.release();

	_overflowBuffer.resize(0);
}


SocketImpl* SecureSocketImpl::acceptConnection(SocketAddress& clientAddr)
{
	StreamSocket ss = _pSocket->acceptConnection(clientAddr);
	Poco::AutoPtr<SecureStreamSocketImpl> pSecureStreamSocketImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(ss.impl()), _pContext);
	pSecureStreamSocketImpl->acceptSSL();
	pSecureStreamSocketImpl->duplicate();
	return pSecureStreamSocketImpl;
}


void SecureSocketImpl::connect(const SocketAddress& address, bool performHandshake)
{
	_state = ST_ERROR;
	_pSocket->connect(address);
	connectSSL(performHandshake);
	_state = ST_DONE;
}


void SecureSocketImpl::connect(const SocketAddress& address, const Poco::Timespan& timeout, bool performHandshake)
{
	_state = ST_ERROR;
	_pSocket->connect(address, timeout);
	connectSSL(performHandshake);
	_state = ST_DONE;
}


void SecureSocketImpl::connectNB(const SocketAddress& address)
{
	try
	{
		_state = ST_CONNECTING;
		_pSocket->connectNB(address);
	}
	catch (...)
	{
		_state = ST_ERROR;
	}
}


void SecureSocketImpl::bind(const SocketAddress& address, bool reuseAddress)
{
	_pSocket->bind(address, reuseAddress);
}


void SecureSocketImpl::listen(int backlog)
{
	_mode = MODE_SERVER;

	if (_hCreds.dwLower == 0 && _hCreds.dwUpper == 0)
	{
		initServerContext();
	}
	_pSocket->listen(backlog);
}


void SecureSocketImpl::shutdown()
{
	if (_mode == MODE_SERVER)
		serverDisconnect(&_hCreds, &_hContext);
	else
		clientDisconnect(&_hCreds, &_hContext);

	_pSocket->shutdown();
}


void SecureSocketImpl::close()
{
	if (_mode == MODE_SERVER)
		serverDisconnect(&_hCreds, &_hContext);
	else
		clientDisconnect(&_hCreds, &_hContext);
	
	_pSocket->close();
	cleanup();
}


void SecureSocketImpl::abort()
{
	_pSocket->shutdown();
	cleanup();
}


int SecureSocketImpl::available() const
{
	return static_cast<int>(_overflowBuffer.size() + _recvBufferOffset);
}


void SecureSocketImpl::acceptSSL()
{
	_state = ST_DONE;
	initServerContext();
	_needHandshake = true;
}


void SecureSocketImpl::verifyPeerCertificate()
{
	if (_peerHostName.empty())
		_peerHostName = _pSocket->peerAddress().host().toString();
		
	verifyPeerCertificate(_peerHostName);
}


void SecureSocketImpl::verifyPeerCertificate(const std::string& hostName)
{
	Context::VerificationMode mode = _pContext->verificationMode();
	if (mode == Context::VERIFY_NONE || !_pContext->extendedCertificateVerificationEnabled() ||
	    (mode != Context::VERIFY_STRICT && isLocalHost(hostName)))
	{
		return;
	}


	if (_mode == MODE_SERVER)
		serverVerifyCertificate();
	else
		clientVerifyCertificate(hostName);
}


bool SecureSocketImpl::isLocalHost(const std::string& hostName)
{
	SocketAddress addr(hostName, 0);
	return addr.host().isLoopback();
}


int SecureSocketImpl::sendRawBytes(const void* buffer, int length, int flags)
{
	return _pSocket->sendBytes(buffer, length, flags);
}


int SecureSocketImpl::receiveRawBytes(void* buffer, int length, int flags)
{
	return _pSocket->receiveBytes(buffer, length, flags);
}


int SecureSocketImpl::sendBytes(const void* buffer, int length, int flags)
{
	if (_needHandshake)
	{
		completeHandshake();
		_needHandshake = false;
	}

	if (_state == ST_ERROR) return 0;

	if (_state != ST_DONE)
	{
		bool establish = _pSocket->getBlocking();
		if (establish)
		{
			while (_state != ST_DONE)
			{
				stateMachine();
			}
		}
		else
		{
			stateMachine();
			return -1;
		}
	}

	int rc = 0;
	int dataToSend = length;
	int dataSent = 0;
	const char* pBuffer = reinterpret_cast<const char*>(buffer);

	if (_sendBuffer.capacity() != _ioBufferSize)
		_sendBuffer.setCapacity(_ioBufferSize);

	while (dataToSend > 0)
	{
		AutoSecBufferDesc<4> msg(&_securityFunctions, false);
		int dataSize = dataToSend;
		if (dataSize > _streamSizes.cbMaximumMessage)
			dataSize = _streamSizes.cbMaximumMessage;
		SecBuffer* pDataBuffer  = 0;
		SecBuffer* pExtraBuffer = 0;

		std::memcpy(_sendBuffer.begin() + _streamSizes.cbHeader, pBuffer + dataSent, dataSize);
		
		msg.setSecBufferStreamHeader(0, _sendBuffer.begin(), _streamSizes.cbHeader);
		msg.setSecBufferData(1, _sendBuffer.begin() + _streamSizes.cbHeader, dataSize);
		msg.setSecBufferStreamTrailer(2, _sendBuffer.begin() + _streamSizes.cbHeader + dataSize, _streamSizes.cbTrailer);
		msg.setSecBufferEmpty(3);

		SECURITY_STATUS securityStatus = _securityFunctions.EncryptMessage(&_hContext, 0, &msg, 0);

		if (FAILED(securityStatus) && securityStatus != SEC_E_CONTEXT_EXPIRED)
			throw SSLException("Failed to encrypt message", Utility::formatError(securityStatus));

		int outBufferLen = msg[0].cbBuffer + msg[1].cbBuffer + msg[2].cbBuffer;

		int sent = sendRawBytes(_sendBuffer.begin(), outBufferLen, flags);
		if (_pSocket->getBlocking() && sent == -1)
		{
			if (dataSent == 0) 
				return -1;
			else
				return dataSent;
		}
		if (sent != outBufferLen)
			throw SSLException("Failed to send encrypted message");

		dataToSend -= dataSize;
		dataSent += dataSize;
		rc += sent;
	}
	return dataSent;
}


int SecureSocketImpl::receiveBytes(void* buffer, int length, int flags)
{
	if (_needHandshake)
	{
		completeHandshake();
		_needHandshake = false;
	}

	if (_state == ST_ERROR) return 0;
	if (_state != ST_DONE)
	{
		bool establish = _pSocket->getBlocking();
		if (establish)
		{
			while (_state != ST_DONE)
			{
				stateMachine();
			}
		}
		else
		{
			stateMachine();
			return -1;
		}
	}

	int rc(0);
	std::size_t overflowSize = _overflowBuffer.size();
	if (overflowSize > 0) // any remaining data from previous calls?
	{
		if (static_cast<DWORD>(length) < overflowSize)
		{
			rc = length;
			std::memcpy(buffer, _overflowBuffer.begin(), rc);
			std::memmove(_overflowBuffer.begin(), _overflowBuffer.begin() + rc, overflowSize - rc);
			_overflowBuffer.resize(overflowSize - rc);
		}
		else
		{
			rc = overflowSize;
			std::memcpy(buffer, _overflowBuffer.begin(), rc);
			_overflowBuffer.resize(0);
		}
	}
	else
	{
		// adjust buffer size to optimize memory usage
		if (_ioBufferSize != _recvBuffer.capacity() && _recvBufferOffset < _ioBufferSize)
		{
			_recvBuffer.setCapacity(_ioBufferSize);
		}
		bool needData = _recvBufferOffset == 0;
		bool cont = true;
		do
		{
			if (needData)
			{
				int numBytes = receiveRawBytes(_recvBuffer.begin() + _recvBufferOffset, _ioBufferSize - _recvBufferOffset);
					
				if (numBytes == -1)
					return -1;
				else if (numBytes == 0) 
					break;
				else
					_recvBufferOffset += numBytes;
			}
			else needData = true;

			int bytesDecoded = 0;
			_extraSecBuffer.pvBuffer = 0;
			_extraSecBuffer.cbBuffer = 0;
			SECURITY_STATUS securityStatus = decodeBufferFull(_recvBuffer.begin(), _recvBufferOffset, reinterpret_cast<char*>(buffer), length, bytesDecoded);
			if (_extraSecBuffer.cbBuffer > 0)
			{
				std::memmove(_recvBuffer.begin(), _extraSecBuffer.pvBuffer, _extraSecBuffer.cbBuffer);
				_recvBufferOffset = _extraSecBuffer.cbBuffer;
			}
			else
			{
				_recvBufferOffset = 0;
				cont = false;
			}

			_extraSecBuffer.pvBuffer = 0;
			_extraSecBuffer.cbBuffer = 0;

			if (bytesDecoded > 0)
			{
				// bytesDecoded contains everything including overflow data
				rc = bytesDecoded;
				if (rc > length) 
					rc = length;
				return rc;
			}

			if (securityStatus == SEC_E_INCOMPLETE_MESSAGE)
			{
				if (!_pSocket->getBlocking())
					return -1;
				continue;
			}

			if (securityStatus == SEC_I_CONTEXT_EXPIRED)
			{
				SetLastError(securityStatus);
				break;
			}

			if (securityStatus != SEC_E_OK && securityStatus != SEC_I_RENEGOTIATE && securityStatus != SEC_I_CONTEXT_EXPIRED)
			{
				SetLastError(securityStatus);
				break;
			}

			if (securityStatus == SEC_I_RENEGOTIATE)
			{
				_needData = false;
				_state = ST_CLIENTHANDSHAKECONDREAD;
				if (!_pSocket->getBlocking())
					return -1;

				securityStatus = performClientHandshakeLoop();

				if (securityStatus != SEC_E_OK)
					break;

				if (_extraSecBuffer.pvBuffer)
				{
					std::memmove(_recvBuffer.begin(), _extraSecBuffer.pvBuffer, _extraSecBuffer.cbBuffer);
					_recvBufferOffset = _extraSecBuffer.cbBuffer;
				}

				_extraSecBuffer.pvBuffer = 0;
				_extraSecBuffer.cbBuffer = 0;
			}
		}
		while (cont);
	}

	return rc;
}


SECURITY_STATUS SecureSocketImpl::decodeMessage(BYTE* pBuffer, DWORD bufSize, AutoSecBufferDesc<4>& msg, SecBuffer*& pDataBuffer, SecBuffer*& pExtraBuffer)
{
	msg.setSecBufferData(0, pBuffer, bufSize);
	msg.setSecBufferEmpty(1);
	msg.setSecBufferEmpty(2);
	msg.setSecBufferEmpty(3);
	pDataBuffer  = 0;
	pExtraBuffer = 0;

	SECURITY_STATUS securityStatus = _securityFunctions.DecryptMessage(&_hContext, &msg, 0, 0);

	if (securityStatus == SEC_E_OK || securityStatus == SEC_I_RENEGOTIATE)
	{
		for (int i = 1; i < 4; ++i)
		{
			if (pDataBuffer == 0 && msg[i].BufferType == SECBUFFER_DATA)
				pDataBuffer = &msg[i];

			if (pExtraBuffer == NULL && msg[i].BufferType == SECBUFFER_EXTRA)
				pExtraBuffer = &msg[i];
		}
	}

	return securityStatus;
}


SECURITY_STATUS SecureSocketImpl::decodeBufferFull(BYTE* pBuffer, DWORD bufSize, char* pOutBuffer, int outLength, int& bytesDecoded)
{
	poco_check_ptr (pBuffer);
	poco_assert (bufSize > 0);
	poco_check_ptr (pOutBuffer);
	poco_assert (outLength > 0);

	_extraSecBuffer.pvBuffer = 0;
	_extraSecBuffer.cbBuffer = 0;

	SecBuffer* pDataBuffer = 0;
	SecBuffer* pExtraBuffer = 0;
	int bytes = 0;
	bytesDecoded = 0;

	Poco::Buffer<BYTE> overflowBuffer(0);
	int overflowOffset = 0;
	SECURITY_STATUS securityStatus = SEC_E_OK;
	do
	{
		AutoSecBufferDesc<4> msg(&_securityFunctions, false);
		securityStatus = decodeMessage(pBuffer, bufSize, msg, pDataBuffer, pExtraBuffer);
		if (pDataBuffer && pDataBuffer->cbBuffer > 0)
		{
			bytes = pDataBuffer->cbBuffer;
			bytesDecoded += bytes;
			// do we have room for more data in pOutBuffer?
			if (bytes <= outLength) // yes, everything fits in there
			{
				outLength -= bytes;
				std::memcpy(pOutBuffer, pDataBuffer->pvBuffer, bytes);
				pOutBuffer += bytes;
			}
			else
			{
				// not enough room in pOutBuffer, write overflow data
				// simply reserve bufSize bytes (is large enough even in worst case scenario, no need to re-increase)
				overflowBuffer.resize(bufSize);
				if (outLength > 0)
				{
					// make pOutBuffer full 
					std::memcpy(pOutBuffer, pDataBuffer->pvBuffer, outLength);
					// no longer valid to write to pOutBuffer
					pOutBuffer = 0;
					// copy the rest to ptrOverflow
					std::memcpy(overflowBuffer.begin(), reinterpret_cast<char*>(pDataBuffer->pvBuffer) + outLength, bytes - outLength);
					overflowOffset = bytes - outLength;
					outLength = 0;
				}
				else
				{
					// append to overflowBuffer
					poco_assert_dbg (overflowOffset + bytes <= overflowBuffer.capacity());
					std::memcpy(overflowBuffer.begin() + overflowOffset, pDataBuffer->pvBuffer, bytes);
					overflowOffset += bytes;
				}
			}
		}
		if (pExtraBuffer && pExtraBuffer->cbBuffer > 0)
		{
			// we have potentially more data to decode
			// decode as much as possible
			pBuffer = reinterpret_cast<BYTE*>(pExtraBuffer->pvBuffer);
			bufSize = pExtraBuffer->cbBuffer;
		}
		else
		{
			// everything decoded
			if (securityStatus != SEC_E_OK && securityStatus != SEC_E_INCOMPLETE_MESSAGE && securityStatus != SEC_I_RENEGOTIATE && securityStatus != SEC_I_CONTEXT_EXPIRED)
			{
				throw SSLException("Failed to decode data", Utility::formatError(securityStatus));
			}
			else if (securityStatus == SEC_E_OK)
			{
				pBuffer = 0;
				bufSize = 0;
			}
		}

		if (securityStatus == SEC_I_RENEGOTIATE) 
		{
			_needData = false;
			securityStatus = performClientHandshakeLoop();
			if (securityStatus != SEC_E_OK)
				break;
		}
	}
	while (securityStatus == SEC_E_OK && pBuffer);
	
	if (overflowOffset > 0)
	{
		_overflowBuffer.resize(overflowOffset);
		std::memcpy(_overflowBuffer.begin(), overflowBuffer.begin(), overflowOffset);
	}
	if (bufSize > 0)
	{
		_extraSecBuffer.cbBuffer = bufSize;
		_extraSecBuffer.pvBuffer = pBuffer;
	}

	if (pBuffer == 0) securityStatus = SEC_E_OK;
	return securityStatus;
}


void SecureSocketImpl::setPeerHostName(const std::string& peerHostName)
{
	_peerHostName = peerHostName;
}


PCCERT_CONTEXT SecureSocketImpl::loadCertificate(bool mustFindCertificate)
{
	try
	{
		Poco::Net::X509Certificate cert = _pContext->certificate();
		PCCERT_CONTEXT pCert = cert.system();
		CertDuplicateCertificateContext(pCert);
		return pCert;
	}
	catch (...)
	{
		if (mustFindCertificate)
			throw;
		else
			return 0;
	}
}


void SecureSocketImpl::connectSSL(bool completeHandshake)
{
	poco_assert_dbg(_pPeerCertificate == 0);

	if (_peerHostName.empty())
	{
		_peerHostName = _pSocket->address().host().toString();
	}

	initClientContext();
	if (completeHandshake)
	{
		performClientHandshake();
		_needHandshake = false;
	}
	else
	{
		_needHandshake = true;
	}
}


void SecureSocketImpl::completeHandshake()
{
	if (_mode == MODE_SERVER)
		performServerHandshake();
	else
		performClientHandshake();
}


void SecureSocketImpl::clientConnectVerify()
{
	poco_assert_dbg(!_pPeerCertificate);
	poco_assert_dbg(!_peerHostName.empty());

	try
	{
		SECURITY_STATUS securityStatus = _securityFunctions.QueryContextAttributesW(&_hContext, SECPKG_ATTR_REMOTE_CERT_CONTEXT, (PVOID) &_pPeerCertificate);
		if (securityStatus != SEC_E_OK) 
			throw SSLException("Failed to obtain peer certificate", Utility::formatError(securityStatus));

		clientVerifyCertificate(_peerHostName);

		securityStatus = _securityFunctions.QueryContextAttributesW(&_hContext, SECPKG_ATTR_STREAM_SIZES, &_streamSizes);
		if (securityStatus != SEC_E_OK)
			throw SSLException("Failed to query stream sizes", Utility::formatError(securityStatus));

		_ioBufferSize = _streamSizes.cbHeader + _streamSizes.cbMaximumMessage + _streamSizes.cbTrailer;
		_state = ST_DONE;
	}
	catch (...)
	{
		if (_pPeerCertificate)
		{
			CertFreeCertificateContext(_pPeerCertificate);
			_pPeerCertificate = 0;
		}
		throw;
	}
}


void SecureSocketImpl::initClientContext()
{
	_pOwnCertificate = loadCertificate(false);
	_hCreds = _pContext->credentials();
}


void SecureSocketImpl::performClientHandshake()
{
	performInitialClientHandshake();
	performClientHandshakeLoop();
	clientConnectVerify();
}


void SecureSocketImpl::performInitialClientHandshake()
{
	// get initial security token
	_outSecBuffer.reset(true);
	_outSecBuffer.setSecBufferToken(0, 0, 0);

	TimeStamp ts;
	DWORD contextAttributes(0);
	std::wstring whostName;
	Poco::UnicodeConverter::convert(_peerHostName, whostName);
	_securityStatus = _securityFunctions.InitializeSecurityContextW(
						&_hCreds,
						0,
						const_cast<SEC_WCHAR*>(whostName.c_str()),
						_contextFlags,
						0,
						0,
						0,
						0,
						&_hContext, 
						&_outSecBuffer,
						&contextAttributes,
						&ts);

	if (_securityStatus != SEC_E_OK)
	{
		if (_securityStatus == SEC_I_INCOMPLETE_CREDENTIALS)
		{
			// the server is asking for client credentials, we didn't send one because we were not configured to do so, abort
			throw SSLException("Handshake failed: No client credentials configured");
		}
		else if (_securityStatus != SEC_I_CONTINUE_NEEDED)
		{
			throw SSLException("Handshake failed", Utility::formatError(_securityStatus));
		}
	}
	
	// incomplete credentials: more calls to InitializeSecurityContext needed
	// send the token
	sendInitialTokenOutBuffer();

	if (_securityStatus == SEC_E_OK)
	{
		// The security context was successfully initialized. 
		// There is no need for another InitializeSecurityContext (Schannel) call. 
		_state = ST_DONE;
		return;
	}

	//SEC_I_CONTINUE_NEEDED was returned:
	// Wait for a return token. The returned token is then passed in 
	// another call to InitializeSecurityContext (Schannel). The output token can be empty.

	_extraSecBuffer.pvBuffer = 0;
	_extraSecBuffer.cbBuffer = 0;
	_needData = true;
	_state = ST_CLIENTHANDSHAKECONDREAD;
	_securityStatus = SEC_E_INCOMPLETE_MESSAGE;
}


void SecureSocketImpl::sendInitialTokenOutBuffer()
{
	// send the token
	if (_outSecBuffer[0].cbBuffer && _outSecBuffer[0].pvBuffer)
	{
		int numBytes = sendRawBytes(_outSecBuffer[0].pvBuffer, _outSecBuffer[0].cbBuffer);
		if (numBytes != _outSecBuffer[0].cbBuffer)
			throw SSLException("Failed to send token to the server");
	}
}


SECURITY_STATUS SecureSocketImpl::performClientHandshakeLoop()
{
	_recvBufferOffset = 0;
	_securityStatus = SEC_E_INCOMPLETE_MESSAGE;

	while (_securityStatus == SEC_I_CONTINUE_NEEDED || _securityStatus == SEC_E_INCOMPLETE_MESSAGE || _securityStatus == SEC_I_INCOMPLETE_CREDENTIALS)
	{
		performClientHandshakeLoopCondReceive();
		
		if (_securityStatus == SEC_E_OK)
		{
			performClientHandshakeLoopOK();
		}
		else if (_securityStatus == SEC_I_CONTINUE_NEEDED)
		{
			performClientHandshakeLoopContinueNeeded();
		}
		else if (_securityStatus == SEC_E_INCOMPLETE_MESSAGE)
		{
			performClientHandshakeLoopIncompleteMessage();
		}
		else if (FAILED(_securityStatus))
		{
			if (_outFlags & ISC_RET_EXTENDED_ERROR)
			{
				performClientHandshakeLoopExtError();
			}
			else
			{
				performClientHandshakeLoopError();
			}
		}
		else
		{
			performClientHandshakeLoopIncompleteMessage();
		}
	}

	if (FAILED(_securityStatus))
	{
		performClientHandshakeLoopError();
	}

	return _securityStatus;
}


void SecureSocketImpl::performClientHandshakeLoopExtError()
{
	poco_assert_dbg (FAILED(_securityStatus));

	performClientHandshakeSendOutBuffer();
	performClientHandshakeLoopError();
}


void SecureSocketImpl::performClientHandshakeLoopError()
{
	poco_assert_dbg (FAILED(_securityStatus));
	cleanup();
	_state = ST_ERROR;
	throw SSLException("Error during handshake", Utility::formatError(_securityStatus));
}


void SecureSocketImpl::performClientHandshakeSendOutBuffer()
{
	if (_outSecBuffer[0].cbBuffer && _outSecBuffer[0].pvBuffer) 
	{
		int numBytes = sendRawBytes(static_cast<const void*>(_outSecBuffer[0].pvBuffer), _outSecBuffer[0].cbBuffer);
		if (numBytes != _outSecBuffer[0].cbBuffer)
			throw SSLException("Socket error during handshake");

		_outSecBuffer.release(0);
	}
}


void SecureSocketImpl::performClientHandshakeExtraBuffer()
{
	if (_inSecBuffer[1].BufferType == SECBUFFER_EXTRA)
	{
		std::memmove(_recvBuffer.begin(), _recvBuffer.begin() + (_recvBufferOffset - _inSecBuffer[1].cbBuffer), _inSecBuffer[1].cbBuffer);
		_recvBufferOffset = _inSecBuffer[1].cbBuffer;
	}
	else _recvBufferOffset = 0;
}


void SecureSocketImpl::performClientHandshakeLoopOK()
{
	poco_assert_dbg(_securityStatus == SEC_E_OK);

	performClientHandshakeSendOutBuffer();
	performClientHandshakeExtraBuffer();
	_state = ST_VERIFY;
}


void SecureSocketImpl::performClientHandshakeLoopInit()
{
	_inSecBuffer.reset(false);
	_outSecBuffer.reset(true);
}


void SecureSocketImpl::performClientHandshakeLoopReceive()
{
	poco_assert_dbg (_needData);
	poco_assert (IO_BUFFER_SIZE > _recvBufferOffset);

	int n = receiveRawBytes(_recvBuffer.begin() + _recvBufferOffset, IO_BUFFER_SIZE - _recvBufferOffset);
	if (n <= 0) throw SSLException("Error during handshake: failed to read data");

	_recvBufferOffset += n;
}


void SecureSocketImpl::performClientHandshakeLoopCondReceive()
{
	poco_assert_dbg (_securityStatus == SEC_E_INCOMPLETE_MESSAGE || SEC_I_CONTINUE_NEEDED);
	
	performClientHandshakeLoopInit();
	if (_needData)
	{
		performClientHandshakeLoopReceive();
	}
	else _needData = true;
		
	_inSecBuffer.setSecBufferToken(0, _recvBuffer.begin(), _recvBufferOffset);
	// inbuffer 1 should be empty
	_inSecBuffer.setSecBufferEmpty(1);

	// outBuffer[0] should be empty
	_outSecBuffer.setSecBufferToken(0, 0, 0);

	_outFlags = 0;
	TimeStamp ts;
	_securityStatus = _securityFunctions.InitializeSecurityContextW(
								&_hCreds,
								&_hContext,
								0,
								_contextFlags,
								0,
								0,
								&_inSecBuffer,
								0,
								0,
								&_outSecBuffer,
								&_outFlags,
								&ts);

	if (_securityStatus == SEC_E_OK)
	{
		_state = ST_CLIENTHANDSHAKEOK;
	}
	else if (_securityStatus == SEC_I_CONTINUE_NEEDED)
	{
		_state = ST_CLIENTHANDSHAKECONTINUE;
	}
	else if (FAILED(_securityStatus))
	{
		if (_outFlags & ISC_RET_EXTENDED_ERROR)
			_state = ST_CLIENTHANDSHAKEEXTERROR;
		else
			_state = ST_ERROR;
	}
	else
	{
		_state = ST_CLIENTHANDSHAKEINCOMPLETE;
	}
}


void SecureSocketImpl::performClientHandshakeLoopContinueNeeded()
{
	performClientHandshakeSendOutBuffer();
	performClientHandshakeExtraBuffer();
	_state = ST_CLIENTHANDSHAKECONDREAD;
}


void SecureSocketImpl::performClientHandshakeLoopIncompleteMessage()
{
	_needData = true;
	_state = ST_CLIENTHANDSHAKECONDREAD;
}


void SecureSocketImpl::initServerContext()
{
	_pOwnCertificate = loadCertificate(true);
	_hCreds = _pContext->credentials();
}


void SecureSocketImpl::performServerHandshake()
{
	serverHandshakeLoop(&_hContext, &_hCreds, _clientAuthRequired, true, true);

	SECURITY_STATUS securityStatus;
	if (_clientAuthRequired) 
	{
		poco_assert_dbg (!_pPeerCertificate);
		securityStatus = _securityFunctions.QueryContextAttributesW(&_hContext, SECPKG_ATTR_REMOTE_CERT_CONTEXT, &_pPeerCertificate);

		if (securityStatus != SEC_E_OK)
		{
			if (_pPeerCertificate)
			{
				CertFreeCertificateContext(_pPeerCertificate);
				_pPeerCertificate = 0;
			}
			throw SSLException("Cannot obtain client certificate", Utility::formatError(securityStatus));
		}
		else
		{
			serverVerifyCertificate();
		}
	}

	securityStatus = _securityFunctions.QueryContextAttributesW(&_hContext,SECPKG_ATTR_STREAM_SIZES, &_streamSizes);
	if (securityStatus != SEC_E_OK) throw SSLException("Cannot query stream sizes", Utility::formatError(securityStatus));

	_ioBufferSize = _streamSizes.cbHeader + _streamSizes.cbMaximumMessage + _streamSizes.cbTrailer;
}


bool SecureSocketImpl::serverHandshakeLoop(PCtxtHandle phContext, PCredHandle phCred, bool requireClientAuth, bool doInitialRead, bool newContext)
{
	TimeStamp tsExpiry;
	int n = 0;
	bool doRead = doInitialRead;
	bool initContext = newContext;
	DWORD outFlags;
	SECURITY_STATUS securityStatus = SEC_E_INCOMPLETE_MESSAGE;

	while (securityStatus == SEC_I_CONTINUE_NEEDED || securityStatus == SEC_E_INCOMPLETE_MESSAGE || securityStatus == SEC_I_INCOMPLETE_CREDENTIALS)
	{
		if (securityStatus == SEC_E_INCOMPLETE_MESSAGE) 
		{
			if (doRead)
			{
				n = receiveRawBytes(_recvBuffer.begin() + _recvBufferOffset, IO_BUFFER_SIZE - _recvBufferOffset);

				if (n <= 0)
					throw SSLException("Failed to receive data in handshake");
				else
					_recvBufferOffset += n;
			} 
			else doRead = true;
		}

		AutoSecBufferDesc<2> inBuffer(&_securityFunctions, false);
		AutoSecBufferDesc<1> outBuffer(&_securityFunctions, true);
		inBuffer.setSecBufferToken(0, _recvBuffer.begin(), _recvBufferOffset);
		inBuffer.setSecBufferEmpty(1);
		outBuffer.setSecBufferToken(0, 0, 0);

		securityStatus = _securityFunctions.AcceptSecurityContext(
						phCred,
						initContext ? NULL : phContext,
						&inBuffer,
						_contextFlags,
						0,
						initContext ? phContext : NULL,
						&outBuffer,
						&outFlags,
						&tsExpiry);

		initContext = false;

		if (securityStatus == SEC_E_OK || securityStatus == SEC_I_CONTINUE_NEEDED || (FAILED(securityStatus) && (0 != (outFlags & ISC_RET_EXTENDED_ERROR))))
		{
			if (outBuffer[0].cbBuffer != 0 && outBuffer[0].pvBuffer != 0)
			{
				n = sendRawBytes(outBuffer[0].pvBuffer, outBuffer[0].cbBuffer);
				outBuffer.release(0);
			}
		}

		if (securityStatus == SEC_E_OK )
		{
			if (inBuffer[1].BufferType == SECBUFFER_EXTRA)
			{
				std::memmove(_recvBuffer.begin(), _recvBuffer.begin() + (_recvBufferOffset - inBuffer[1].cbBuffer), inBuffer[1].cbBuffer);
				_recvBufferOffset = inBuffer[1].cbBuffer;
			} 
			else 
			{
				_recvBufferOffset = 0;
			}
			return true;
		}
		else if (FAILED(securityStatus) && securityStatus != SEC_E_INCOMPLETE_MESSAGE)
		{
			throw SSLException("Handshake failure:", Utility::formatError(securityStatus));
		}

		if (securityStatus != SEC_E_INCOMPLETE_MESSAGE && securityStatus != SEC_I_INCOMPLETE_CREDENTIALS)
		{
			if (inBuffer[1].BufferType == SECBUFFER_EXTRA)
			{
				std::memmove(_recvBuffer.begin(), _recvBuffer.begin() + (_recvBufferOffset - inBuffer[1].cbBuffer), inBuffer[1].cbBuffer);
				_recvBufferOffset = inBuffer[1].cbBuffer;
			}
			else 
			{
				_recvBufferOffset = 0;
			}
		}
	}

	return false;
}


void SecureSocketImpl::clientVerifyCertificate(const std::string& hostName)
{
	if (_pContext->verificationMode() == Context::VERIFY_NONE) return;	
	if (!_pPeerCertificate) throw SSLException("No Server certificate");
	if (hostName.empty()) throw SSLException("Server name not set");

	X509Certificate cert(_pPeerCertificate, true);
	
	if (!cert.verify(hostName))
	{
		VerificationErrorArgs args(cert, 0, SEC_E_CERT_EXPIRED, "The certificate host names do not match the server host name");
		SSLManager::instance().ClientVerificationError(this, args);
		if (!args.getIgnoreError())
			throw InvalidCertificateException("Host name verification failed");
	}

	verifyCertificateChainClient(_pPeerCertificate);	
}


void SecureSocketImpl::verifyCertificateChainClient(PCCERT_CONTEXT pServerCert)
{
	X509Certificate cert(pServerCert, true);

	CERT_CHAIN_PARA chainPara;
	PCCERT_CHAIN_CONTEXT pChainContext = NULL;
	std::memset(&chainPara, 0, sizeof(chainPara));
	chainPara.cbSize = sizeof(chainPara);

	if (!CertGetCertificateChain(
							NULL,
							_pPeerCertificate,
							NULL,
							NULL,
							&chainPara,
							0,
							NULL,
							&pChainContext))
	{
		throw SSLException("Cannot get certificate chain", GetLastError());
	}

	HTTPSPolicyCallbackData polHttps; 
	std::memset(&polHttps, 0, sizeof(HTTPSPolicyCallbackData));
	polHttps.cbStruct = sizeof(HTTPSPolicyCallbackData);
	polHttps.dwAuthType = AUTHTYPE_SERVER;
	polHttps.fdwChecks = SECURITY_FLAG_IGNORE_UNKNOWN_CA; // we do our own check later on
	polHttps.pwszServerName = 0;

	CERT_CHAIN_POLICY_PARA polPara;
	std::memset(&polPara, 0, sizeof(polPara));
	polPara.cbSize = sizeof(polPara);
	polPara.pvExtraPolicyPara = &polHttps;

	CERT_CHAIN_POLICY_STATUS polStatus;
	std::memset(&polStatus, 0, sizeof(polStatus));
	polStatus.cbSize = sizeof(polStatus);

	if (!CertVerifyCertificateChainPolicy(
						CERT_CHAIN_POLICY_SSL,
						pChainContext,
						&polPara,
						&polStatus))
	{
		VerificationErrorArgs args(cert, 0, GetLastError(), "Failed to verify certificate chain");
		SSLManager::instance().ClientVerificationError(this, args);
		if (!args.getIgnoreError())
		{
			CertFreeCertificateChain(pChainContext);
			throw SSLException("Cannot verify certificate chain");
		}
		else return;
	}
	else if (polStatus.dwError)
	{
		VerificationErrorArgs args(cert, polStatus.lElementIndex, polStatus.dwError, Utility::formatError(polStatus.dwError));
		SSLManager::instance().ClientVerificationError(this, args);
		CertFreeCertificateChain(pChainContext);
		if (!args.getIgnoreError())
		{
			throw SSLException("Failed to verify certificate chain");
		}
		else return;
	}

	// now verify CA's
	HCERTSTORE trustedCerts = _pContext->certificateStore();
	for (DWORD i = 0; i < pChainContext->cChain; i++)
	{
		std::vector<PCCERT_CONTEXT> certs;
		for (DWORD k = 0; k < pChainContext->rgpChain[i]->cElement; k++)
		{
			certs.push_back(pChainContext->rgpChain[i]->rgpElement[k]->pCertContext);
		}
		// verify that the root of the chain can be found in the trusted store
		PCCERT_CONTEXT pResult = CertFindCertificateInStore(trustedCerts, certs.back()->dwCertEncodingType, 0, CERT_FIND_ISSUER_OF, certs.back(), 0);
		if (!pResult)
		{
			poco_assert_dbg (GetLastError() == CRYPT_E_NOT_FOUND);
			VerificationErrorArgs args(cert, i, 0, "Certificate Authority not trusted");
			SSLManager::instance().ClientVerificationError(this, args);
			CertFreeCertificateChain(pChainContext);
			if (!args.getIgnoreError())
				throw CertificateValidationException("Failed to verify certificate chain: CA not trusted");
			else
				return;
		}
		CertFreeCertificateContext(pResult);

#if !defined(_WIN32_WCE)
		// check if cert is revoked
		if (_pContext->options() & Context::OPT_PERFORM_REVOCATION_CHECK)
		{
			CERT_REVOCATION_STATUS revStat;
			revStat.cbSize = sizeof(CERT_REVOCATION_STATUS);

			BOOL ok = CertVerifyRevocation(
					X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
					CERT_CONTEXT_REVOCATION_TYPE,
					certs.size(),
					(void**) &certs[0],
					CERT_VERIFY_REV_CHAIN_FLAG,
					NULL,
					&revStat);

			// Revocation check of the root certificate may fail due to missing CRL points, etc.
			// We ignore all errors checking the root certificate except CRYPT_E_REVOKED.
			if (!ok && (revStat.dwIndex < certs.size() - 1 || revStat.dwError == CRYPT_E_REVOKED))
			{
				VerificationErrorArgs args(cert, revStat.dwIndex, revStat.dwReason, Utility::formatError(revStat.dwError));
				SSLManager::instance().ClientVerificationError(this, args);
				if (!args.getIgnoreError())
				{
					CertFreeCertificateChain(pChainContext);
					throw SSLException("Failed to verify revoked certificate chain");
				}
			}
			else break;
		}
#endif
	}
	CertFreeCertificateChain(pChainContext);
}


void SecureSocketImpl::serverVerifyCertificate()
{
	if (_pContext->verificationMode() < Context::VERIFY_STRICT) return;	

	// we are now in Strict mode
	if (!_pPeerCertificate) throw SSLException("No client certificate");

	DWORD status = SEC_E_OK;
	X509Certificate cert(_pPeerCertificate, true);
	
	PCCERT_CHAIN_CONTEXT pChainContext = NULL;
	CERT_CHAIN_PARA chainPara;
	std::memset(&chainPara, 0, sizeof(chainPara));
	chainPara.cbSize = sizeof(chainPara);

	if (!CertGetCertificateChain(
							NULL,
							_pPeerCertificate,
							NULL,
							NULL,
							&chainPara,
							CERT_CHAIN_REVOCATION_CHECK_CHAIN,
							NULL,
							&pChainContext)) 
	{
		throw SSLException("Cannot get certificate chain", GetLastError());
	}

	HTTPSPolicyCallbackData polHttps;
	std::memset(&polHttps, 0, sizeof(HTTPSPolicyCallbackData));
	polHttps.cbStruct       = sizeof(HTTPSPolicyCallbackData);
	polHttps.dwAuthType     = AUTHTYPE_CLIENT;
	polHttps.fdwChecks      = 0;
	polHttps.pwszServerName = 0;

	CERT_CHAIN_POLICY_PARA policyPara;
	std::memset(&policyPara, 0, sizeof(policyPara));
	policyPara.cbSize = sizeof(policyPara);
	policyPara.pvExtraPolicyPara = &polHttps;

	CERT_CHAIN_POLICY_STATUS policyStatus;
	std::memset(&policyStatus, 0, sizeof(policyStatus));
	policyStatus.cbSize = sizeof(policyStatus);

	if (!CertVerifyCertificateChainPolicy(CERT_CHAIN_POLICY_SSL, pChainContext, &policyPara, &policyStatus)) 
	{
		VerificationErrorArgs args(cert, 0, GetLastError(), "Failed to verify certificate chain");
		SSLManager::instance().ServerVerificationError(this, args);
		CertFreeCertificateChain(pChainContext);
		if (!args.getIgnoreError()) 
			throw SSLException("Cannot verify certificate chain");
		else
			return;
	}
	else if (policyStatus.dwError) 
	{
		VerificationErrorArgs args(cert, policyStatus.lElementIndex, status, Utility::formatError(policyStatus.dwError));
		SSLManager::instance().ServerVerificationError(this, args);
		CertFreeCertificateChain(pChainContext);
		if (!args.getIgnoreError())
			throw SSLException("Failed to verify certificate chain");
		else 
			return;
	}

#if !defined(_WIN32_WCE)
	// perform revocation checking
	for (DWORD i = 0; i < pChainContext->cChain; i++) 
	{
		std::vector<PCCERT_CONTEXT> certs;
		for (DWORD k = 0; k < pChainContext->rgpChain[i]->cElement; k++)
		{
			certs.push_back(pChainContext->rgpChain[i]->rgpElement[k]->pCertContext);
		}
	
		CERT_REVOCATION_STATUS revStat;
		revStat.cbSize = sizeof(CERT_REVOCATION_STATUS);

		BOOL ok = CertVerifyRevocation(
						X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
						CERT_CONTEXT_REVOCATION_TYPE,
						certs.size(),
						(void**) &certs[0],
						CERT_VERIFY_REV_CHAIN_FLAG,
						NULL,
						&revStat);
		if (!ok && (revStat.dwIndex < certs.size() - 1 || revStat.dwError == CRYPT_E_REVOKED))
		{
			VerificationErrorArgs args(cert, revStat.dwIndex, revStat.dwReason, Utility::formatError(revStat.dwReason));
			SSLManager::instance().ServerVerificationError(this, args);
			if (!args.getIgnoreError())
			{
				CertFreeCertificateChain(pChainContext);
				throw SSLException("Failed to verify certificate chain");
			}
		}
	}
#endif
	if (pChainContext) 
	{
		CertFreeCertificateChain(pChainContext);
	}
}


LONG SecureSocketImpl::clientDisconnect(PCredHandle phCreds, CtxtHandle* phContext)
{
	if (phContext->dwLower == 0 && phContext->dwUpper == 0)
	{
		return SEC_E_OK;
	}

	AutoSecBufferDesc<1> tokBuffer(&_securityFunctions, false);

	DWORD tokenType = SCHANNEL_SHUTDOWN;
	tokBuffer.setSecBufferToken(0, &tokenType, sizeof(tokenType));
	DWORD status = _securityFunctions.ApplyControlToken(phContext, &tokBuffer);

	if (FAILED(status)) return status;

	DWORD sspiFlags = ISC_REQ_SEQUENCE_DETECT
                    | ISC_REQ_REPLAY_DETECT
	                | ISC_REQ_CONFIDENTIALITY
	                | ISC_RET_EXTENDED_ERROR
	                | ISC_REQ_ALLOCATE_MEMORY
	                | ISC_REQ_STREAM;

	AutoSecBufferDesc<1> outBuffer(&_securityFunctions, true);
	outBuffer.setSecBufferToken(0, 0, 0);

	DWORD sspiOutFlags;
	TimeStamp expiry;
	status = _securityFunctions.InitializeSecurityContextW(
				phCreds,
				phContext,
				NULL,
				sspiFlags,
				0,
				0,
				NULL,
				0,
				phContext,
				&outBuffer,
				&sspiOutFlags,
				&expiry);

	return status;
}


LONG SecureSocketImpl::serverDisconnect(PCredHandle phCreds, CtxtHandle* phContext)
{
	if (phContext->dwLower == 0 && phContext->dwUpper == 0)
	{
		// handshake has never been done
		poco_assert_dbg (_needHandshake);
		return SEC_E_OK;
	}

	AutoSecBufferDesc<1> tokBuffer(&_securityFunctions, false);
	
	DWORD tokenType = SCHANNEL_SHUTDOWN;
	tokBuffer.setSecBufferToken(0, &tokenType, sizeof(tokenType));
	DWORD status = _securityFunctions.ApplyControlToken(phContext, &tokBuffer);

	if (FAILED(status)) return status;

	DWORD sspiFlags = ASC_REQ_SEQUENCE_DETECT
	                | ASC_REQ_REPLAY_DETECT 
	                | ASC_REQ_CONFIDENTIALITY 
	                | ASC_REQ_EXTENDED_ERROR 
	                | ASC_REQ_ALLOCATE_MEMORY
	                | ASC_REQ_STREAM;

	AutoSecBufferDesc<1> outBuffer(&_securityFunctions, true);
	outBuffer.setSecBufferToken(0,0,0);

	DWORD sspiOutFlags;
	TimeStamp expiry;
	status = _securityFunctions.AcceptSecurityContext(
					phCreds,
					phContext,
					NULL,
					sspiFlags,
					0,
					NULL,
					&outBuffer,
					&sspiOutFlags,
					&expiry);

	if (FAILED(status)) return status;

	if (outBuffer[0].pvBuffer && outBuffer[0].cbBuffer)
	{
		int sent = sendRawBytes(outBuffer[0].pvBuffer, outBuffer[0].cbBuffer);
		if (sent <= 0)
		{
			status = WSAGetLastError();
		}
	}

	return status;
}


void SecureSocketImpl::stateIllegal()
{
	throw Poco::IllegalStateException("SSL state machine");
}


void SecureSocketImpl::stateConnected()
{
	_peerHostName = _pSocket->address().host().toString();
	initClientContext();
	performInitialClientHandshake();
}


void SecureSocketImpl::stateMachine()
{
	StateMachine::instance().execute(this);
}


namespace
{
	static Poco::SingletonHolder<StateMachine> stateMachineSingleton;
}


StateMachine& StateMachine::instance()
{
	return *stateMachineSingleton.get();
}


bool StateMachine::readable(SOCKET sockfd)
{
	fd_set fdRead;
	FD_ZERO(&fdRead);
	FD_SET(sockfd, &fdRead);
	select(&fdRead, 0, sockfd);
	return (FD_ISSET(sockfd, &fdRead) != 0);
}


bool StateMachine::writable(SOCKET sockfd)
{
	fd_set fdWrite;
	FD_ZERO(&fdWrite);
	FD_SET(sockfd, &fdWrite);
	select(0, &fdWrite, sockfd);
	return (FD_ISSET(sockfd, &fdWrite) != 0);
}


bool StateMachine::readOrWritable(SOCKET sockfd)
{
	fd_set fdRead, fdWrite;
	FD_ZERO(&fdRead);
	FD_SET(sockfd, &fdRead);
	fdWrite = fdRead;
	select(&fdRead, &fdWrite, sockfd);
	return (FD_ISSET(sockfd, &fdRead) != 0 || FD_ISSET(sockfd, &fdWrite) != 0);
}


bool StateMachine::none(SOCKET sockfd)
{
	return true;
}


void StateMachine::select(fd_set* fdRead, fd_set* fdWrite, SOCKET sockfd)
{
	Poco::Timespan remainingTime(((Poco::Timestamp::TimeDiff)SecureSocketImpl::TIMEOUT_MILLISECS)*1000);
	int rc(0);
	do
	{
		struct timeval tv;
		tv.tv_sec  = (long) remainingTime.totalSeconds();
		tv.tv_usec = (long) remainingTime.useconds();
		Poco::Timestamp start;
		rc = ::select(int(sockfd) + 1, fdRead, fdWrite, 0, &tv);
		if (rc < 0 && SecureSocketImpl::lastError() == POCO_EINTR)
		{
			Poco::Timestamp end;
			Poco::Timespan waited = end - start;
			if (waited < remainingTime)
				remainingTime -= waited;
			else
				remainingTime = 0;
		}
	}
	while (rc < 0 && SecureSocketImpl::lastError() == POCO_EINTR);
}


StateMachine::StateMachine():
	_states()
{
	//ST_INITIAL: 0, -> this one is illegal, you must call connectNB before
	_states.push_back(std::make_pair(&StateMachine::none, &SecureSocketImpl::stateIllegal));
	//ST_CONNECTING: connectNB was called, check if the socket is already available for writing
	_states.push_back(std::make_pair(&StateMachine::writable, &SecureSocketImpl::stateConnected));
	//ST_ESTABLISHTUNNELRECEIVED: we got the response, now start the handshake
	_states.push_back(std::make_pair(&StateMachine::writable, &SecureSocketImpl::performInitialClientHandshake));
	//ST_CLIENTHANDSHAKECONDREAD: condread
	_states.push_back(std::make_pair(&StateMachine::readable, &SecureSocketImpl::performClientHandshakeLoopCondReceive));
	//ST_CLIENTHANDSHAKEINCOMPLETE,
	_states.push_back(std::make_pair(&StateMachine::none, &SecureSocketImpl::performClientHandshakeLoopIncompleteMessage));
	//ST_CLIENTHANDSHAKEOK,
	_states.push_back(std::make_pair(&StateMachine::writable, &SecureSocketImpl::performClientHandshakeLoopOK));
	//ST_CLIENTHANDSHAKEEXTERROR,
	_states.push_back(std::make_pair(&StateMachine::writable, &SecureSocketImpl::performClientHandshakeLoopExtError));
	//ST_CLIENTHANDSHAKECONTINUE,
	_states.push_back(std::make_pair(&StateMachine::writable, &SecureSocketImpl::performClientHandshakeLoopContinueNeeded));
	//ST_VERIFY,
	_states.push_back(std::make_pair(&StateMachine::none, &SecureSocketImpl::clientConnectVerify));
	//ST_DONE,
	_states.push_back(std::make_pair(&StateMachine::none, &SecureSocketImpl::stateIllegal));
	//ST_ERROR
	_states.push_back(std::make_pair(&StateMachine::none, &SecureSocketImpl::performClientHandshakeLoopError));
}


void StateMachine::execute(SecureSocketImpl* pSock)
{
	try
	{
		poco_assert_dbg (pSock);
		ConditionState& state = _states[pSock->getState()];
		ConditionMethod& meth = state.first;
		if ((this->*state.first)(pSock->sockfd()))
		{
			(pSock->*(state.second))();
			(pSock->getState() == SecureSocketImpl::ST_DONE);
		}
	}
	catch (...)
	{
		pSock->setState(SecureSocketImpl::ST_ERROR);
		throw;
	}
}


StateMachine::~StateMachine()
{
}


} } // namespace Poco::Net
