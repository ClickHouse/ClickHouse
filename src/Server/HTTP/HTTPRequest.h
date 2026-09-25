#pragma once

#include <Poco/Net/HTTPRequest.h>
#include <Poco/String.h>

#include <string>

namespace DB
{

using HTTPRequest = Poco::Net::HTTPRequest;

/// Media type names are case-insensitive. Compare only the type/subtype portion so a value such as
/// `multipart/form-datafoo` is not treated as `multipart/form-data`.
inline bool isContentType(const HTTPRequest & request, const char * expected)
{
    const auto & content_type = request.getContentType();
    const std::string media_type = Poco::trim(content_type.substr(0, content_type.find(';')));
    return Poco::icompare(media_type, expected) == 0;
}

inline bool isMultipartFormData(const HTTPRequest & request)
{
    return isContentType(request, "multipart/form-data");
}

inline bool isUrlEncodedFormData(const HTTPRequest & request)
{
    return isContentType(request, "application/x-www-form-urlencoded");
}

}
