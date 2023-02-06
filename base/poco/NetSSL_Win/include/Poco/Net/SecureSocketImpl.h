//
// SecureSocketImpl.h
//
// Library: NetSSL_Win
// Package: SSLSockets
// Module:  SecureSocketImpl
//
// Definition of the SecureSocketImpl class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_SecureSocketImpl_INCLUDED
#define NetSSL_SecureSocketImpl_INCLUDED


#include "Poco/Net/SocketImpl.h"
#include "Poco/Net/NetSSL.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/AutoSecBufferDesc.h"
#include "Poco/Net/X509Certificate.h"
#include "Poco/Buffer.h"
#include <winsock2.h>
#include <windows.h>
#include <wincrypt.h>
#include <schannel.h>
#ifndef SECURITY_WIN32
#define SECURITY_WIN32
#endif
#include <security.h>
#include <sspi.h>


namespace Poco {
namespace Net {


class NetSSL_Win_API SecureSocketImpl
	/// The SocketImpl for SecureStreamSocket.
{
public:
	enum Mode
	{
		MODE_CLIENT,
		MODE_SERVER
	};

	SecureSocketImpl(Poco::AutoPtr<SocketImpl> pSocketImpl, Context::Ptr pContext);
		/// Creates the SecureSocketImpl.

	virtual ~SecureSocketImpl();
		/// Destroys the SecureSocketImpl.

	SocketImpl* acceptConnection(SocketAddress& clientAddr);
		/// Get the next completed connection from the
		/// socket's completed connection queue.
		///
		/// If the queue is empty, waits until a connection
		/// request completes.
		///
		/// Returns a new TCP socket for the connection
		/// with the client.
		///
		/// The client socket's address is returned in clientAddr.
	
	void connect(const SocketAddress& address, bool performHandshake);
		/// Initializes the socket and establishes a connection to 
		/// the TCP server at the given address.
		///
		/// Can also be used for UDP sockets. In this case, no
		/// connection is established. Instead, incoming and outgoing
		/// packets are restricted to the specified address.

	void connect(const SocketAddress& address, const Poco::Timespan& timeout, bool performHandshake);
		/// Initializes the socket, sets the socket timeout and 
		/// establishes a connection to the TCP server at the given address.

	void connectNB(const SocketAddress& address);
		/// Initializes the socket and establishes a connection to 
		/// the TCP server at the given address. Prior to opening the
		/// connection the socket is set to nonblocking mode.
	
	void bind(const SocketAddress& address, bool reuseAddress = false);
		/// Bind a local address to the socket.
		///
		/// This is usually only done when establishing a server
		/// socket. TCP clients should not bind a socket to a
		/// specific address.
		///
		/// If reuseAddress is true, sets the SO_REUSEADDR
		/// socket option.
		
	void listen(int backlog = 64);
		/// Puts the socket into listening state.
		///
		/// The socket becomes a passive socket that
		/// can accept incoming connection requests.
		///
		/// The backlog argument specifies the maximum
		/// number of connections that can be queued
		/// for this socket.

	void shutdown();
		/// Shuts down the connection by attempting
		/// an orderly SSL shutdown, then actually
		/// shutting down the TCP connection.

	void close();
		/// Close the socket.

	void abort();
		/// Aborts the connection by closing the
		/// underlying TCP connection. No orderly SSL shutdown
		/// is performed.
	
	int sendBytes(const void* buffer, int length, int flags = 0);
		/// Sends the contents of the given buffer through
		/// the socket. Any specified flags are ignored.
		///
		/// Returns the number of bytes sent, which may be
		/// less than the number of bytes specified.
	
	int receiveBytes(void* buffer, int length, int flags = 0);
		/// Receives data from the socket and stores it
		/// in buffer. Up to length bytes are received.
		///
		/// Returns the number of bytes received.

	void setPeerHostName(const std::string& hostName);
		/// Sets the peer host name for certificate validation purposes.
		
	const std::string& getPeerHostName() const;
		/// Returns the peer host name.

	void verifyPeerCertificate();
		/// Performs post-connect (or post-accept) peer certificate validation,
		/// using the peer host name set with setPeerHostName(), or the peer's
		/// IP address string if no peer host name has been set.

	void verifyPeerCertificate(const std::string& hostName);
		/// Performs post-connect (or post-accept) peer certificate validation
		/// using the given peer host name.

	Context::Ptr context() const;
		/// Returns the Context.

	PCCERT_CONTEXT peerCertificate() const;
		/// Returns the peer certificate.

	poco_socket_t sockfd();
		/// Returns the underlying socket descriptor.

	int available() const;
		/// Returns the number of bytes available in the buffer.

protected:
	enum
	{
		IO_BUFFER_SIZE    = 32768,
		TIMEOUT_MILLISECS = 200
	};

	enum State
	{
		ST_INITIAL = 0,
		ST_CONNECTING,
		ST_CLIENTHANDSHAKESTART,
		ST_CLIENTHANDSHAKECONDREAD,
		ST_CLIENTHANDSHAKEINCOMPLETE,
		ST_CLIENTHANDSHAKEOK,
		ST_CLIENTHANDSHAKEEXTERROR,
		ST_CLIENTHANDSHAKECONTINUE,
		ST_VERIFY,
		ST_DONE,
		ST_ERROR
	};

	int sendRawBytes(const void* buffer, int length, int flags = 0);
	int receiveRawBytes(void* buffer, int length, int flags = 0);
	void clientConnectVerify();
	void sendInitialTokenOutBuffer();
	void performServerHandshake();
	bool serverHandshakeLoop(PCtxtHandle phContext, PCredHandle phCred, bool requireClientAuth, bool doInitialRead, bool newContext);
	void clientVerifyCertificate(const std::string& hostName);
	void verifyCertificateChainClient(PCCERT_CONTEXT pServerCert);
	void serverVerifyCertificate();
	LONG serverDisconnect(PCredHandle phCreds, CtxtHandle* phContext);
	LONG clientDisconnect(PCredHandle phCreds, CtxtHandle* phContext);
	bool loadSecurityLibrary();
	void initClientContext();
	void initServerContext();
	PCCERT_CONTEXT loadCertificate(bool mustFindCertificate);
	void initCommon();
	void cleanup();
	void performClientHandshake();
	void performInitialClientHandshake();
	SECURITY_STATUS performClientHandshakeLoop();
	void performClientHandshakeLoopIncompleteMessage();
	void performClientHandshakeLoopCondReceive();
	void performClientHandshakeLoopReceive();
	void performClientHandshakeLoopOK();
	void performClientHandshakeLoopInit();
	void performClientHandshakeExtraBuffer();
	void performClientHandshakeSendOutBuffer();
	void performClientHandshakeLoopContinueNeeded();
	void performClientHandshakeLoopError();
	void performClientHandshakeLoopExtError();
	SECURITY_STATUS decodeMessage(BYTE* pBuffer, DWORD bufSize, AutoSecBufferDesc<4>& msg, SecBuffer*& pData, SecBuffer*& pExtra);
	SECURITY_STATUS decodeBufferFull(BYTE* pBuffer, DWORD bufSize, char* pOutBuffer, int outLength, int& bytesDecoded);
	void stateIllegal();
	void stateConnected();
	void acceptSSL();
	void connectSSL(bool completeHandshake);
	void completeHandshake();
	static int lastError();
	void stateMachine();
	State getState() const;
	void setState(State st);
	static bool isLocalHost(const std::string& hostName);

private:
	SecureSocketImpl(const SecureSocketImpl&);
	SecureSocketImpl& operator = (const SecureSocketImpl&);

	Poco::AutoPtr<SocketImpl> _pSocket;
	Context::Ptr   _pContext;
	Mode           _mode;
	std::string    _peerHostName;
	bool           _useMachineStore;
	bool           _clientAuthRequired;

	SecurityFunctionTableW& _securityFunctions;

	PCCERT_CONTEXT _pOwnCertificate;
	PCCERT_CONTEXT _pPeerCertificate;

	CredHandle _hCreds;
	CtxtHandle _hContext;
	DWORD      _contextFlags;

	Poco::Buffer<BYTE> _overflowBuffer;
	Poco::Buffer<BYTE> _sendBuffer;
	Poco::Buffer<BYTE> _recvBuffer;
	DWORD _recvBufferOffset;
	DWORD _ioBufferSize;

	SecPkgContext_StreamSizes _streamSizes;
	AutoSecBufferDesc<1> _outSecBuffer;
	AutoSecBufferDesc<2> _inSecBuffer;
	SecBuffer _extraSecBuffer;
	SECURITY_STATUS _securityStatus;
	State _state;
	DWORD _outFlags;
	bool _needData;
	bool _needHandshake;

	friend class SecureStreamSocketImpl;
	friend class StateMachine;
};


//
// inlines
//
inline poco_socket_t SecureSocketImpl::sockfd()
{
	return _pSocket->sockfd();
}


inline Context::Ptr SecureSocketImpl::context() const
{
	return _pContext;
}


inline SecureSocketImpl::State SecureSocketImpl::getState() const
{
	return _state;
}


inline void SecureSocketImpl::setState(SecureSocketImpl::State st)
{
	_state = st;
}


inline const std::string& SecureSocketImpl::getPeerHostName() const
{
	return _peerHostName;
}


inline PCCERT_CONTEXT SecureSocketImpl::peerCertificate() const
{
	return _pPeerCertificate;
}


inline int SecureSocketImpl::lastError() 
{
	return SocketImpl::lastError();
}


} } // namespace Poco::Net


#endif // NetSSL_SecureSocketImpl_INCLUDED
