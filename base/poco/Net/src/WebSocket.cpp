//
// WebSocket.cpp
//
// Library: Net
// Package: WebSocket
// Module:  WebSocket
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/WebSocket.h"
#include "Poco/Net/WebSocketImpl.h"
#include "Poco/Net/HTTPServerRequestImpl.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPClientSession.h"
#include "Poco/Net/HTTPServerSession.h"
#include "Poco/Net/NetException.h"
#include "Poco/MemoryStream.h"
#include "Poco/NullStream.h"
#include "Poco/BinaryWriter.h"
#include "Poco/SHA1Engine.h"
#include "Poco/Base64Encoder.h"
#include "Poco/String.h"
#include "Poco/Random.h"
#include "Poco/StreamCopier.h"
#include <sstream>


namespace Poco {
namespace Net {


const std::string WebSocket::WEBSOCKET_GUID("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
const std::string WebSocket::WEBSOCKET_VERSION("13");
HTTPCredentials WebSocket::_defaultCreds;


WebSocket::WebSocket(HTTPServerRequest& request, HTTPServerResponse& response):
	StreamSocket(accept(request, response))
{
}


WebSocket::WebSocket(HTTPClientSession& cs, HTTPRequest& request, HTTPResponse& response):
	StreamSocket(connect(cs, request, response, _defaultCreds))
{
}


WebSocket::WebSocket(HTTPClientSession& cs, HTTPRequest& request, HTTPResponse& response, HTTPCredentials& credentials):
	StreamSocket(connect(cs, request, response, credentials))
{
}


WebSocket::WebSocket(const Socket& socket):
	StreamSocket(socket)
{
	if (!dynamic_cast<WebSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


WebSocket::~WebSocket()
{
}


WebSocket& WebSocket::operator = (const Socket& socket)
{
	if (dynamic_cast<WebSocketImpl*>(socket.impl()))
		Socket::operator = (socket);
	else
		throw InvalidArgumentException("Cannot assign incompatible socket");
	return *this;
}


void WebSocket::shutdown()
{
	shutdown(WS_NORMAL_CLOSE);
}


void WebSocket::shutdown(Poco::UInt16 statusCode, const std::string& statusMessage)
{
	Poco::Buffer<char> buffer(statusMessage.size() + 2);
	Poco::MemoryOutputStream ostr(buffer.begin(), buffer.size());
	Poco::BinaryWriter writer(ostr, Poco::BinaryWriter::NETWORK_BYTE_ORDER);
	writer << statusCode;
	writer.writeRaw(statusMessage);
	sendFrame(buffer.begin(), static_cast<int>(ostr.charsWritten()), FRAME_FLAG_FIN | FRAME_OP_CLOSE);
}


int WebSocket::sendFrame(const void* buffer, int length, int flags)
{
	flags |= FRAME_OP_SETRAW;
	return static_cast<WebSocketImpl*>(impl())->sendBytes(buffer, length, flags);
}


int WebSocket::receiveFrame(void* buffer, int length, int& flags)
{
	int n = static_cast<WebSocketImpl*>(impl())->receiveBytes(buffer, length, 0);
	flags = static_cast<WebSocketImpl*>(impl())->frameFlags();
	return n;
}


int WebSocket::receiveFrame(Poco::Buffer<char>& buffer, int& flags)
{
	int n = static_cast<WebSocketImpl*>(impl())->receiveBytes(buffer, 0);
	flags = static_cast<WebSocketImpl*>(impl())->frameFlags();
	return n;
}


WebSocket::Mode WebSocket::mode() const
{
	return static_cast<WebSocketImpl*>(impl())->mustMaskPayload() ? WS_CLIENT : WS_SERVER;
}


void WebSocket::setMaxPayloadSize(int maxPayloadSize)
{
	static_cast<WebSocketImpl*>(impl())->setMaxPayloadSize(maxPayloadSize);
}


int WebSocket::getMaxPayloadSize() const
{
	return static_cast<WebSocketImpl*>(impl())->getMaxPayloadSize();
}


WebSocketImpl* WebSocket::accept(HTTPServerRequest& request, HTTPServerResponse& response)
{
	if (request.hasToken("Connection", "upgrade") && icompare(request.get("Upgrade", ""), "websocket") == 0)
	{
		std::string version = request.get("Sec-WebSocket-Version", "");
		if (version.empty()) throw WebSocketException("Missing Sec-WebSocket-Version in handshake request", WS_ERR_HANDSHAKE_NO_VERSION);
		if (version != WEBSOCKET_VERSION) throw WebSocketException("Unsupported WebSocket version requested", version, WS_ERR_HANDSHAKE_UNSUPPORTED_VERSION);
		std::string key = request.get("Sec-WebSocket-Key", "");
		Poco::trimInPlace(key);
		if (key.empty()) throw WebSocketException("Missing Sec-WebSocket-Key in handshake request", WS_ERR_HANDSHAKE_NO_KEY);

		response.setStatusAndReason(HTTPResponse::HTTP_SWITCHING_PROTOCOLS);
		response.set("Upgrade", "websocket");
		response.set("Connection", "Upgrade");
		response.set("Sec-WebSocket-Accept", computeAccept(key));
		response.setContentLength(HTTPResponse::UNKNOWN_CONTENT_LENGTH);
		response.send().flush();

		HTTPServerRequestImpl& requestImpl = static_cast<HTTPServerRequestImpl&>(request);
		return new WebSocketImpl(static_cast<StreamSocketImpl*>(requestImpl.detachSocket().impl()), requestImpl.session(), false);
	}
	else throw WebSocketException("No WebSocket handshake", WS_ERR_NO_HANDSHAKE);
}


WebSocketImpl* WebSocket::connect(HTTPClientSession& cs, HTTPRequest& request, HTTPResponse& response, HTTPCredentials& credentials)
{
	if (!cs.getProxyHost().empty() && !cs.secure())
	{
		cs.proxyTunnel();
	}
	std::string key = createKey();
	request.set("Connection", "Upgrade");
	request.set("Upgrade", "websocket");
	request.set("Sec-WebSocket-Version", WEBSOCKET_VERSION);
	request.set("Sec-WebSocket-Key", key);
	request.setChunkedTransferEncoding(false);
	cs.setKeepAlive(true);
	cs.sendRequest(request);
	std::istream& istr = cs.receiveResponse(response);
	if (response.getStatus() == HTTPResponse::HTTP_SWITCHING_PROTOCOLS)
	{
		return completeHandshake(cs, response, key);
	}
	else if (response.getStatus() == HTTPResponse::HTTP_UNAUTHORIZED)
	{
		if (!credentials.empty())
		{
			Poco::NullOutputStream null;
			Poco::StreamCopier::copyStream(istr, null);
			credentials.authenticate(request, response);
			if (!cs.getProxyHost().empty() && !cs.secure())
			{
				cs.reset();
				cs.proxyTunnel();
			}
			cs.sendRequest(request);
			cs.receiveResponse(response);
			if (response.getStatus() == HTTPResponse::HTTP_SWITCHING_PROTOCOLS)
			{
				return completeHandshake(cs, response, key);
			}
			else if (response.getStatus() == HTTPResponse::HTTP_UNAUTHORIZED)
			{
				throw WebSocketException("Not authorized", WS_ERR_UNAUTHORIZED);
			}
		}
		else throw WebSocketException("Not authorized", WS_ERR_UNAUTHORIZED);
	}
	if (response.getStatus() == HTTPResponse::HTTP_OK)
	{
		throw WebSocketException("The server does not understand the WebSocket protocol", WS_ERR_NO_HANDSHAKE);
	}
	else
	{
		throw WebSocketException("Cannot upgrade to WebSocket connection", response.getReason(), WS_ERR_NO_HANDSHAKE);
	}
}


WebSocketImpl* WebSocket::completeHandshake(HTTPClientSession& cs, HTTPResponse& response, const std::string& key)
{
	std::string connection = response.get("Connection", "");
	if (Poco::icompare(connection, "Upgrade") != 0)
		throw WebSocketException("No Connection: Upgrade header in handshake response", WS_ERR_NO_HANDSHAKE);
	std::string upgrade = response.get("Upgrade", "");
	if (Poco::icompare(upgrade, "websocket") != 0)
		throw WebSocketException("No Upgrade: websocket header in handshake response", WS_ERR_NO_HANDSHAKE);
	std::string accept = response.get("Sec-WebSocket-Accept", "");
	if (accept != computeAccept(key))
		throw WebSocketException("Invalid or missing Sec-WebSocket-Accept header in handshake response", WS_ERR_HANDSHAKE_ACCEPT);
	return new WebSocketImpl(static_cast<StreamSocketImpl*>(cs.detachSocket().impl()), cs, true);
}


std::string WebSocket::createKey()
{
	Poco::Random rnd;
	std::ostringstream ostr;
	Poco::Base64Encoder base64(ostr);
	Poco::BinaryWriter writer(base64);
	writer << rnd.next() << rnd.next() << rnd.next() << rnd.next();
	base64.close();
	return ostr.str();
}


std::string WebSocket::computeAccept(const std::string& key)
{
	std::string accept(key);
	accept += WEBSOCKET_GUID;
	Poco::SHA1Engine sha1;
	sha1.update(accept);
	Poco::DigestEngine::Digest d = sha1.digest();
	std::ostringstream ostr;
	Poco::Base64Encoder base64(ostr);
	base64.write(reinterpret_cast<const char*>(&d[0]), d.size());
	base64.close();
	return ostr.str();
}


} } // namespace Poco::Net
