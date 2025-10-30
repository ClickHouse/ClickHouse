//
// HTTPResponse.h
//
// Library: Net
// Package: HTTP
// Module:  HTTPResponse
//
// Definition of the HTTPResponse class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPResponse_INCLUDED
#define Net_HTTPResponse_INCLUDED


#include <map>
#include <vector>

#include "Poco/Net/HTTPCookie.h"
#include "Poco/Net/HTTPMessage.h"
#include "Poco/Net/Net.h"
#include "Poco/Timestamp.h"


namespace Poco
{
namespace Net
{


    class HTTPCookie;


    class Net_API HTTPResponse : public HTTPMessage
    /// This class encapsulates an HTTP response
    /// message.
    ///
    /// In addition to the properties common to
    /// all HTTP messages, a HTTP response has
    /// status code and a reason phrase.
    {
    public:
        enum HTTPStatus
        {
            HTTP_CONTINUE = 100,
            HTTP_SWITCHING_PROTOCOLS = 101,
            HTTP_PROCESSING = 102,
            HTTP_OK = 200,
            HTTP_CREATED = 201,
            HTTP_ACCEPTED = 202,
            HTTP_NONAUTHORITATIVE = 203,
            HTTP_NO_CONTENT = 204,
            HTTP_RESET_CONTENT = 205,
            HTTP_PARTIAL_CONTENT = 206,
            HTTP_MULTI_STATUS = 207,
            HTTP_ALREADY_REPORTED = 208,
            HTTP_IM_USED = 226,
            HTTP_MULTIPLE_CHOICES = 300,
            HTTP_MOVED_PERMANENTLY = 301,
            HTTP_FOUND = 302,
            HTTP_SEE_OTHER = 303,
            HTTP_NOT_MODIFIED = 304,
            HTTP_USE_PROXY = 305,
            HTTP_USEPROXY = 305, /// @deprecated
            // UNUSED: 306
            HTTP_TEMPORARY_REDIRECT = 307,
            HTTP_PERMANENT_REDIRECT = 308,
            HTTP_BAD_REQUEST = 400,
            HTTP_UNAUTHORIZED = 401,
            HTTP_PAYMENT_REQUIRED = 402,
            HTTP_FORBIDDEN = 403,
            HTTP_NOT_FOUND = 404,
            HTTP_METHOD_NOT_ALLOWED = 405,
            HTTP_NOT_ACCEPTABLE = 406,
            HTTP_PROXY_AUTHENTICATION_REQUIRED = 407,
            HTTP_REQUEST_TIMEOUT = 408,
            HTTP_CONFLICT = 409,
            HTTP_GONE = 410,
            HTTP_LENGTH_REQUIRED = 411,
            HTTP_PRECONDITION_FAILED = 412,
            HTTP_REQUEST_ENTITY_TOO_LARGE = 413,
            HTTP_REQUESTENTITYTOOLARGE = 413, /// @deprecated
            HTTP_REQUEST_URI_TOO_LONG = 414,
            HTTP_REQUESTURITOOLONG = 414, /// @deprecated
            HTTP_UNSUPPORTED_MEDIA_TYPE = 415,
            HTTP_UNSUPPORTEDMEDIATYPE = 415, /// @deprecated
            HTTP_REQUESTED_RANGE_NOT_SATISFIABLE = 416,
            HTTP_EXPECTATION_FAILED = 417,
            HTTP_IM_A_TEAPOT = 418,
            HTTP_ENCHANCE_YOUR_CALM = 420,
            HTTP_MISDIRECTED_REQUEST = 421,
            HTTP_UNPROCESSABLE_ENTITY = 422,
            HTTP_LOCKED = 423,
            HTTP_FAILED_DEPENDENCY = 424,
            HTTP_UPGRADE_REQUIRED = 426,
            HTTP_PRECONDITION_REQUIRED = 428,
            HTTP_TOO_MANY_REQUESTS = 429,
            HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE = 431,
            HTTP_UNAVAILABLE_FOR_LEGAL_REASONS = 451,
            HTTP_INTERNAL_SERVER_ERROR = 500,
            HTTP_NOT_IMPLEMENTED = 501,
            HTTP_BAD_GATEWAY = 502,
            HTTP_SERVICE_UNAVAILABLE = 503,
            HTTP_GATEWAY_TIMEOUT = 504,
            HTTP_VERSION_NOT_SUPPORTED = 505,
            HTTP_VARIANT_ALSO_NEGOTIATES = 506,
            HTTP_INSUFFICIENT_STORAGE = 507,
            HTTP_LOOP_DETECTED = 508,
            HTTP_NOT_EXTENDED = 510,
            HTTP_NETWORK_AUTHENTICATION_REQUIRED = 511
        };

        HTTPResponse();
        /// Creates the HTTPResponse with OK status.

        HTTPResponse(HTTPStatus status, const std::string & reason);
        /// Creates the HTTPResponse with the given status
        /// and reason phrase.

        HTTPResponse(const std::string & version, HTTPStatus status, const std::string & reason);
        /// Creates the HTTPResponse with the given version, status
        /// and reason phrase.

        HTTPResponse(HTTPStatus status);
        /// Creates the HTTPResponse with the given status
        /// an an appropriate reason phrase.

        HTTPResponse(const std::string & version, HTTPStatus status);
        /// Creates the HTTPResponse with the given version, status
        /// an an appropriate reason phrase.

        virtual ~HTTPResponse();
        /// Destroys the HTTPResponse.

        void setStatus(HTTPStatus status);
        /// Sets the HTTP status code.
        ///
        /// Does not change the reason phrase.

        HTTPStatus getStatus() const;
        /// Returns the HTTP status code.

        void setStatus(const std::string & status);
        /// Sets the HTTP status code.
        ///
        /// The string must contain a valid
        /// HTTP numerical status code.

        void setReason(const std::string & reason);
        /// Sets the HTTP reason phrase.

        const std::string & getReason() const;
        /// Returns the HTTP reason phrase.

        void setStatusAndReason(HTTPStatus status, const std::string & reason);
        /// Sets the HTTP status code and reason phrase.

        void setStatusAndReason(HTTPStatus status);
        /// Sets the HTTP status code and reason phrase.
        ///
        /// The reason phrase is set according to the status code.

        void setDate(const Poco::Timestamp & dateTime);
        /// Sets the Date header to the given date/time value.

        Poco::Timestamp getDate() const;
        /// Returns the value of the Date header.

        void addCookie(const HTTPCookie & cookie);
        /// Adds the cookie to the response by
        /// adding a Set-Cookie header.

        void getCookies(std::vector<HTTPCookie> & cookies) const;
        /// Returns a vector with all the cookies
        /// set in the response header.
        ///
        /// May throw an exception in case of a malformed
        /// Set-Cookie header.

        void getHeaders(std::map<std::string, std::string> & headers) const;

        void write(std::ostream & ostr) const;
        /// Writes the HTTP response to the given
        /// output stream.

        void beginWrite(std::ostream & ostr) const;
        /// Writes the HTTP response to the given
        /// output stream, but do not finish with \r\n delimiter.

#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Woverloaded-virtual"
        void read(std::istream & istr);
#    pragma clang diagnostic pop
        /// Reads the HTTP response from the
        /// given input stream.
        ///
        /// 100 Continue responses are ignored.

        static const std::string & getReasonForStatus(HTTPStatus status);
        /// Returns an appropriate reason phrase
        /// for the given status code.

        static const std::string HTTP_REASON_CONTINUE;
        static const std::string HTTP_REASON_SWITCHING_PROTOCOLS;
        static const std::string HTTP_REASON_PROCESSING;
        static const std::string HTTP_REASON_OK;
        static const std::string HTTP_REASON_CREATED;
        static const std::string HTTP_REASON_ACCEPTED;
        static const std::string HTTP_REASON_NONAUTHORITATIVE;
        static const std::string HTTP_REASON_NO_CONTENT;
        static const std::string HTTP_REASON_RESET_CONTENT;
        static const std::string HTTP_REASON_PARTIAL_CONTENT;
        static const std::string HTTP_REASON_MULTI_STATUS;
        static const std::string HTTP_REASON_ALREADY_REPORTED;
        static const std::string HTTP_REASON_IM_USED;
        static const std::string HTTP_REASON_MULTIPLE_CHOICES;
        static const std::string HTTP_REASON_MOVED_PERMANENTLY;
        static const std::string HTTP_REASON_FOUND;
        static const std::string HTTP_REASON_SEE_OTHER;
        static const std::string HTTP_REASON_NOT_MODIFIED;
        static const std::string HTTP_REASON_USE_PROXY;
        static const std::string HTTP_REASON_TEMPORARY_REDIRECT;
        static const std::string HTTP_REASON_PERMANENT_REDIRECT;
        static const std::string HTTP_REASON_BAD_REQUEST;
        static const std::string HTTP_REASON_UNAUTHORIZED;
        static const std::string HTTP_REASON_PAYMENT_REQUIRED;
        static const std::string HTTP_REASON_FORBIDDEN;
        static const std::string HTTP_REASON_NOT_FOUND;
        static const std::string HTTP_REASON_METHOD_NOT_ALLOWED;
        static const std::string HTTP_REASON_NOT_ACCEPTABLE;
        static const std::string HTTP_REASON_PROXY_AUTHENTICATION_REQUIRED;
        static const std::string HTTP_REASON_REQUEST_TIMEOUT;
        static const std::string HTTP_REASON_CONFLICT;
        static const std::string HTTP_REASON_GONE;
        static const std::string HTTP_REASON_LENGTH_REQUIRED;
        static const std::string HTTP_REASON_PRECONDITION_FAILED;
        static const std::string HTTP_REASON_REQUEST_ENTITY_TOO_LARGE;
        static const std::string HTTP_REASON_REQUEST_URI_TOO_LONG;
        static const std::string HTTP_REASON_UNSUPPORTED_MEDIA_TYPE;
        static const std::string HTTP_REASON_REQUESTED_RANGE_NOT_SATISFIABLE;
        static const std::string HTTP_REASON_EXPECTATION_FAILED;
        static const std::string HTTP_REASON_IM_A_TEAPOT;
        static const std::string HTTP_REASON_ENCHANCE_YOUR_CALM;
        static const std::string HTTP_REASON_MISDIRECTED_REQUEST;
        static const std::string HTTP_REASON_UNPROCESSABLE_ENTITY;
        static const std::string HTTP_REASON_LOCKED;
        static const std::string HTTP_REASON_FAILED_DEPENDENCY;
        static const std::string HTTP_REASON_UPGRADE_REQUIRED;
        static const std::string HTTP_REASON_PRECONDITION_REQUIRED;
        static const std::string HTTP_REASON_TOO_MANY_REQUESTS;
        static const std::string HTTP_REASON_REQUEST_HEADER_FIELDS_TOO_LARGE;
        static const std::string HTTP_REASON_UNAVAILABLE_FOR_LEGAL_REASONS;
        static const std::string HTTP_REASON_INTERNAL_SERVER_ERROR;
        static const std::string HTTP_REASON_NOT_IMPLEMENTED;
        static const std::string HTTP_REASON_BAD_GATEWAY;
        static const std::string HTTP_REASON_SERVICE_UNAVAILABLE;
        static const std::string HTTP_REASON_GATEWAY_TIMEOUT;
        static const std::string HTTP_REASON_VERSION_NOT_SUPPORTED;
        static const std::string HTTP_REASON_VARIANT_ALSO_NEGOTIATES;
        static const std::string HTTP_REASON_INSUFFICIENT_STORAGE;
        static const std::string HTTP_REASON_LOOP_DETECTED;
        static const std::string HTTP_REASON_NOT_EXTENDED;
        static const std::string HTTP_REASON_NETWORK_AUTHENTICATION_REQUIRED;
        static const std::string HTTP_REASON_UNKNOWN;

        static const std::string DATE;
        static const std::string SET_COOKIE;

    private:
        enum Limits
        {
            MAX_VERSION_LENGTH = 8,
            MAX_STATUS_LENGTH = 3,
            MAX_REASON_LENGTH = 512
        };

        HTTPStatus _status;
        std::string _reason;

        HTTPResponse(const HTTPResponse &);
        HTTPResponse & operator=(const HTTPResponse &);
    };


    //
    // inlines
    //
    inline HTTPResponse::HTTPStatus HTTPResponse::getStatus() const
    {
        return _status;
    }


    inline const std::string & HTTPResponse::getReason() const
    {
        return _reason;
    }


}
} // namespace Poco::Net


#endif // Net_HTTPResponse_INCLUDED
