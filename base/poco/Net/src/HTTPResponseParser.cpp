//
// HTTPResponseParser.cpp
//
// Library: Net
// Package: HTTP
// Module:  HTTPResponseParser
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPResponseParser.h"
#include "Poco/Ascii.h"
#include "Poco/Exception.h"
#include "Poco/Net/MessageHeader.h"
#include "Poco/Net/NetException.h"
#include "Poco/String.h"


namespace Poco {
namespace Net {


namespace
{
    /// The limits `HTTPResponse::read` applies to the status line.
    constexpr std::size_t MAX_VERSION_LENGTH = 8;
    constexpr std::size_t MAX_STATUS_LENGTH = 3;
    constexpr std::size_t MAX_REASON_LENGTH = 512;

    /// The same test `HTTPResponse::read` applies to a byte it got from `std::istream::get`.
    bool isSpace(char ch)
    {
        return Poco::Ascii::isSpace(static_cast<unsigned char>(ch));
    }
}


HTTPResponseParser::HTTPResponseParser(const HTTPHeaderLimits & limits)
    : _limits(limits)
{
}


void HTTPResponseParser::reset()
{
    _response.clear();
    _response.setVersion(HTTPMessage::HTTP_1_0);
    _response.setStatusAndReason(HTTPResponse::HTTP_OK);
    _state = State::Start;
    _statusLineComplete = false;
    _fields = 0;
    _version.clear();
    _status.clear();
    _reason.clear();
    _name.clear();
    _value.clear();
}


void HTTPResponseParser::reset(const HTTPHeaderLimits & limits)
{
    _limits = limits;
    reset();
}


std::size_t HTTPResponseParser::feed(const char * data, std::size_t size)
{
    std::size_t consumed = 0;
    while (consumed < size && _state != State::Done)
    {
        if (step(data[consumed]))
            ++consumed;
    }
    return consumed;
}


void HTTPResponseParser::feedEof()
{
    switch (_state)
    {
        case State::Start:
            throw NoMessageException();
        case State::LeadingSpace:
            throw MessageException("No HTTP response header");
        case State::Version:
            throw MessageException("Invalid HTTP version string");
        case State::SpaceBeforeStatus:
        case State::Status:
            throw MessageException("Invalid HTTP status code");
        case State::SpaceBeforeReason:
        case State::Reason:
            throw MessageException("HTTP reason string too long");
        case State::StatusLineCR:
            throw MessageException("Unterminated HTTP response line");
        case State::Name:
            throw MessageException("Field name too long/no colon found");
        case State::SpaceBeforeValue:
        case State::Value:
        case State::ValueCR:
        case State::AfterLine:
        case State::FoldValue:
        case State::FoldCR:
            /// The end of the data ends the field and the header.
            finishField();
            break;
        case State::LineStart:
        case State::FinalLine:
            break;
        case State::Done:
            return;
    }
    finishHeader();
    /// The data ended before the final response that a `100 Continue` announces.
    if (_state != State::Done)
        throw NoMessageException();
}


bool HTTPResponseParser::step(char ch)
{
    switch (_state)
    {
        case State::Start:
            _state = State::LeadingSpace;
            return false;

        case State::LeadingSpace:
            /// Empty lines before the status line are allowed.
            if (isSpace(ch))
                return true;
            _state = State::Version;
            return false;

        case State::Version:
            if (!isSpace(ch) && _version.length() < MAX_VERSION_LENGTH)
            {
                _version += ch;
                return true;
            }
            if (!isSpace(ch))
                throw MessageException("Invalid HTTP version string");
            _state = State::SpaceBeforeStatus;
            return false;

        case State::SpaceBeforeStatus:
            if (isSpace(ch))
                return true;
            _state = State::Status;
            return false;

        case State::Status:
            if (!isSpace(ch) && _status.length() < MAX_STATUS_LENGTH)
            {
                _status += ch;
                return true;
            }
            if (!isSpace(ch))
                throw MessageException("Invalid HTTP status code");
            _state = State::SpaceBeforeReason;
            return false;

        case State::SpaceBeforeReason:
            if (isSpace(ch) && ch != '\r' && ch != '\n')
                return true;
            _state = State::Reason;
            return false;

        case State::Reason:
            if (ch != '\r' && ch != '\n' && _reason.length() < MAX_REASON_LENGTH)
            {
                _reason += ch;
                return true;
            }
            if (!isSpace(ch))
                throw MessageException("HTTP reason string too long");
            if (ch == '\r')
            {
                _state = State::StatusLineCR;
                return true;
            }
            if (ch != '\n')
                throw MessageException("Unterminated HTTP response line");
            finishStatusLine();
            return true;

        case State::StatusLineCR:
            if (ch != '\n')
                throw MessageException("Unterminated HTTP response line");
            finishStatusLine();
            return true;

        case State::LineStart:
            if (ch == '\r' || ch == '\n')
            {
                /// The empty line that ends the header: everything up to its line feed belongs to it.
                if (ch == '\n')
                    finishHeader();
                else
                    _state = State::FinalLine;
                return true;
            }
            if (_limits.fields > 0 && _fields == _limits.fields)
                throw MessageException("Too many header fields");
            _name.clear();
            _value.clear();
            _state = State::Name;
            return false;

        case State::Name:
            if (ch != ':' && ch != '\n' && _name.length() < static_cast<std::size_t>(_limits.name_length))
            {
                _name += ch;
                return true;
            }
            if (ch == '\n')
            {
                /// A line without a colon is ignored.
                _state = State::LineStart;
                return true;
            }
            if (ch != ':')
                throw MessageException("Field name too long/no colon found");
            _state = State::SpaceBeforeValue;
            return true;

        case State::SpaceBeforeValue:
            if (isSpace(ch) && ch != '\r' && ch != '\n')
                return true;
            _state = State::Value;
            return false;

        case State::Value:
        case State::FoldValue:
            if (ch != '\r' && ch != '\n' && _value.length() < static_cast<std::size_t>(_limits.value_length))
            {
                _value += ch;
                return true;
            }
            if (ch == '\r')
            {
                _state = _state == State::Value ? State::ValueCR : State::FoldCR;
                return true;
            }
            if (ch == '\n')
            {
                _state = State::AfterLine;
                return true;
            }
            throw MessageException(_state == State::Value ? "Field value too long/no CRLF found" : "Folded field value too long/no CRLF found");

        case State::ValueCR:
        case State::FoldCR:
            if (ch == '\n')
            {
                _state = State::AfterLine;
                return true;
            }
            throw MessageException(_state == State::ValueCR ? "Field value too long/no CRLF found" : "Folded field value too long/no CRLF found");

        case State::AfterLine:
            if (ch == ' ' || ch == '\t')
            {
                /// An obsolete folded line continues the value, with its leading whitespace.
                _state = State::FoldValue;
                return false;
            }
            finishField();
            _state = State::LineStart;
            return false;

        case State::FinalLine:
            if (ch == '\n')
                finishHeader();
            return true;

        case State::Done:
            break;
    }
    throw IllegalStateException("HTTPResponseParser: data after the end of the header");
}


void HTTPResponseParser::finishStatusLine()
{
    try
    {
        _response.setVersion(_version);
        _response.setStatus(_status);
        _response.setReason(_reason);
    }
    catch (const SyntaxException & e)
    {
        throw SyntaxException(e.message(), ", while reading HTTP response: version='" + _version + "', status='" + _status + "', reason='" + _reason + "'");
    }
    _statusLineComplete = true;
    _state = State::LineStart;
}


void HTTPResponseParser::finishField()
{
    Poco::trimRightInPlace(_value);
    _response.add(_name, MessageHeader::decodeWord(_value));
    ++_fields;
}


void HTTPResponseParser::finishHeader()
{
    if (_response.getStatus() == HTTPResponse::HTTP_CONTINUE)
        reset();
    else
        _state = State::Done;
}


}
} // namespace Poco::Net
