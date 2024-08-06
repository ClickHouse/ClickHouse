//
// HTTPMessage.cpp
//
// Library: Net
// Package: HTTP
// Module:  HTTPMessage
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPMessage.h"
#include "Poco/Net/MediaType.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/String.h"
#include <charconv>
#include <format>

using Poco::NumberFormatter;
using Poco::NumberParser;
using Poco::icompare;


namespace Poco {
namespace Net {


const std::string HTTPMessage::HTTP_1_0                   = "HTTP/1.0";
const std::string HTTPMessage::HTTP_1_1                   = "HTTP/1.1";
const std::string HTTPMessage::IDENTITY_TRANSFER_ENCODING = "identity";
const std::string HTTPMessage::CHUNKED_TRANSFER_ENCODING  = "chunked";
const int         HTTPMessage::UNKNOWN_CONTENT_LENGTH     = -1;
const std::string HTTPMessage::UNKNOWN_CONTENT_TYPE;
const std::string HTTPMessage::CONTENT_LENGTH             = "Content-Length";
const std::string HTTPMessage::CONTENT_TYPE               = "Content-Type";
const std::string HTTPMessage::TRANSFER_ENCODING          = "Transfer-Encoding";
const std::string HTTPMessage::CONNECTION                 = "Connection";
const std::string HTTPMessage::CONNECTION_KEEP_ALIVE      = "Keep-Alive";
const std::string HTTPMessage::CONNECTION_CLOSE           = "Close";
const std::string HTTPMessage::EMPTY;


HTTPMessage::HTTPMessage():
	_version(HTTP_1_0)
{
}


HTTPMessage::HTTPMessage(const std::string& version):
	_version(version)
{
}


HTTPMessage::~HTTPMessage()
{
}


void HTTPMessage::setVersion(const std::string& version)
{
	_version = version;
}


void HTTPMessage::setContentLength(std::streamsize length)
{
	if (length != UNKNOWN_CONTENT_LENGTH)
		set(CONTENT_LENGTH, NumberFormatter::format(length));
	else
		erase(CONTENT_LENGTH);
}


std::streamsize HTTPMessage::getContentLength() const
{
	const std::string& contentLength = get(CONTENT_LENGTH, EMPTY);
	if (!contentLength.empty())
	{
		if (sizeof(std::streamsize) == sizeof(Poco::Int64))
			return static_cast<std::streamsize>(NumberParser::parse64(contentLength));
		else
			return static_cast<std::streamsize>(NumberParser::parse(contentLength));
	}
	else return UNKNOWN_CONTENT_LENGTH;
}


void HTTPMessage::setContentLength64(Poco::Int64 length)
{
	if (length != UNKNOWN_CONTENT_LENGTH)
		set(CONTENT_LENGTH, NumberFormatter::format(length));
	else
		erase(CONTENT_LENGTH);
}


Poco::Int64 HTTPMessage::getContentLength64() const
{
	const std::string& contentLength = get(CONTENT_LENGTH, EMPTY);
	if (!contentLength.empty())
	{
		return NumberParser::parse64(contentLength);
	}
	else return UNKNOWN_CONTENT_LENGTH;
}


void HTTPMessage::setTransferEncoding(const std::string& transferEncoding)
{
	if (icompare(transferEncoding, IDENTITY_TRANSFER_ENCODING) == 0)
		erase(TRANSFER_ENCODING);
	else
		set(TRANSFER_ENCODING, transferEncoding);
}


const std::string& HTTPMessage::getTransferEncoding() const
{
	return get(TRANSFER_ENCODING, IDENTITY_TRANSFER_ENCODING);
}


void HTTPMessage::setChunkedTransferEncoding(bool flag)
{
	if (flag)
		setTransferEncoding(CHUNKED_TRANSFER_ENCODING);
	else
		setTransferEncoding(IDENTITY_TRANSFER_ENCODING);
}


bool HTTPMessage::getChunkedTransferEncoding() const
{
	return icompare(getTransferEncoding(), CHUNKED_TRANSFER_ENCODING) == 0;
}


void HTTPMessage::setContentType(const std::string& mediaType)
{
	if (mediaType.empty())
		erase(CONTENT_TYPE);
	else
		set(CONTENT_TYPE, mediaType);
}


void HTTPMessage::setContentType(const MediaType& mediaType)
{
	setContentType(mediaType.toString());
}


const std::string& HTTPMessage::getContentType() const
{
	return get(CONTENT_TYPE, UNKNOWN_CONTENT_TYPE);
}


void HTTPMessage::setKeepAlive(bool keepAlive)
{
	if (keepAlive)
		set(CONNECTION, CONNECTION_KEEP_ALIVE);
	else
		set(CONNECTION, CONNECTION_CLOSE);
}


bool HTTPMessage::getKeepAlive() const
{
	const std::string& connection = get(CONNECTION, EMPTY);
	if (!connection.empty())
		return icompare(connection, CONNECTION_CLOSE) != 0;
	else
		return getVersion() == HTTP_1_1;
}


void HTTPMessage::setKeepAliveTimeout(int timeout, int max_requests)
{
    add(HTTPMessage::CONNECTION_KEEP_ALIVE, std::format("timeout={}, max={}", timeout, max_requests));
}


int parseFromHeaderValues(const std::string_view header_value, const std::string_view param_name)
{
    auto param_value_pos = header_value.find(param_name);
    if (param_value_pos == std::string::npos)
        param_value_pos = header_value.size();
    if (param_value_pos != header_value.size())
        param_value_pos += param_name.size();

    auto param_value_end = header_value.find(',', param_value_pos);
    if (param_value_end == std::string::npos)
        param_value_end = header_value.size();

    auto timeout_value_substr = header_value.substr(param_value_pos, param_value_end - param_value_pos);
    if (timeout_value_substr.empty())
        return -1;

    int value = 0;
    auto [ptr, ec] = std::from_chars(timeout_value_substr.begin(), timeout_value_substr.end(), value);

    if (ec == std::errc())
        return value;

    return -1;
}


int HTTPMessage::getKeepAliveTimeout() const
{
    const std::string& ka_header = get(HTTPMessage::CONNECTION_KEEP_ALIVE, HTTPMessage::EMPTY);
    static const std::string_view timeout_param = "timeout=";
    return parseFromHeaderValues(ka_header, timeout_param);
}


int HTTPMessage::getKeepAliveMaxRequests() const
{
    const std::string& ka_header = get(HTTPMessage::CONNECTION_KEEP_ALIVE, HTTPMessage::EMPTY);
    static const std::string_view timeout_param = "max=";
    return parseFromHeaderValues(ka_header, timeout_param);
}

} } // namespace Poco::Net
