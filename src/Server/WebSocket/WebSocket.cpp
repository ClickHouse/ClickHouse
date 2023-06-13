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


//#include "Poco/Net/WebSocket.h"
#include <Server/WebSocket/WebSocket.h>
#include "Poco/Net/WebSocketImpl.h"
#include "Poco/Net/HTTPServerRequestImpl.h"
//#include "Poco/Net/HTTPServerResponse.h"
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/HTTPServerRequest.h>
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


namespace DB {


const std::string WebSocket::WEBSOCKET_GUID("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
const std::string WebSocket::WEBSOCKET_VERSION("13");
Poco::Net::HTTPCredentials WebSocket::_defaultCreds;


WebSocket::WebSocket(HTTPServerRequest& request, HTTPServerResponse& response):
	Poco::Net::StreamSocket(accept(request, response))
{
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
	return static_cast<Poco::Net::WebSocketImpl*>(impl())->sendBytes(buffer, length, flags);
}


int WebSocket::receiveFrame(void* buffer, int length, int& flags)
{
	int n = static_cast<Poco::Net::WebSocketImpl*>(impl())->receiveBytes(buffer, length, 0);
	flags = static_cast<Poco::Net::WebSocketImpl*>(impl())->frameFlags();
	return n;
}


int WebSocket::receiveFrame(Poco::Buffer<char>& buffer, int& flags)
{
	int n = static_cast<Poco::Net::WebSocketImpl*>(impl())->receiveBytes(buffer, 0);
	flags = static_cast<Poco::Net::WebSocketImpl*>(impl())->frameFlags();
	return n;
}


WebSocket::Mode WebSocket::mode() const
{
	return static_cast<Poco::Net::WebSocketImpl*>(impl())->mustMaskPayload() ? WS_CLIENT : WS_SERVER;
}


void WebSocket::setMaxPayloadSize(int maxPayloadSize)
{
	static_cast<Poco::Net::WebSocketImpl*>(impl())->setMaxPayloadSize(maxPayloadSize);
}


int WebSocket::getMaxPayloadSize() const
{
	return static_cast<Poco::Net::WebSocketImpl*>(impl())->getMaxPayloadSize();
}


Poco::Net::WebSocketImpl* WebSocket::accept(HTTPServerRequest& request, HTTPServerResponse& response)
{
	if (request.hasToken("Connection", "upgrade") && Poco::icompare(request.get("Upgrade", ""), "websocket") == 0)
	{
		std::string version = request.get("Sec-WebSocket-Version", "");
		if (version.empty()) throw Poco::Net::WebSocketException("Missing Sec-WebSocket-Version in handshake request", WS_ERR_HANDSHAKE_NO_VERSION);
		if (version != WEBSOCKET_VERSION) throw Poco::Net::WebSocketException("Unsupported WebSocket version requested", version, WS_ERR_HANDSHAKE_UNSUPPORTED_VERSION);
		std::string key = request.get("Sec-WebSocket-Key", "");
		Poco::trimInPlace(key);
		if (key.empty()) throw Poco::Net::WebSocketException("Missing Sec-WebSocket-Key in handshake request", WS_ERR_HANDSHAKE_NO_KEY);

		response.setStatusAndReason(HTTPResponse::HTTP_SWITCHING_PROTOCOLS);
		response.set("Upgrade", "websocket");
		response.set("Connection", "Upgrade");
		response.set("Sec-WebSocket-Accept", computeAccept(key));
		response.setContentLength(HTTPResponse::UNKNOWN_CONTENT_LENGTH);
		response.send()->flush();


//		Poco::Net::HTTPServerRequestImpl& requestImpl = static_cast<Poco::Net::HTTPServerRequestImpl&>(request);
		return new Poco::Net::WebSocketImpl(static_cast<Poco::Net::StreamSocketImpl*>(request.detachSocket().impl()), request.GetSession(), false);
	}
	else throw Poco::Net::WebSocketException("No WebSocket handshake", WS_ERR_NO_HANDSHAKE);
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


} // namespace DB
