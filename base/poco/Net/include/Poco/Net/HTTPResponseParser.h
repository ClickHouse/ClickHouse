//
// HTTPResponseParser.h
//
// Library: Net
// Package: HTTP
// Module:  HTTPResponseParser
//
// Definition of the HTTPResponseParser class.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPResponseParser_INCLUDED
#define Net_HTTPResponseParser_INCLUDED


#include <cstddef>
#include <string>
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/Net.h"


namespace Poco
{
namespace Net
{


    struct HTTPHeaderLimits
    /// The limits a response header is checked against while it is parsed: the same limits as
    /// the ones of `MessageHeader`, with the same defaults.
    {
        int fields = 100;
        int name_length = 256;
        int value_length = 8192;
    };


    class Net_API HTTPResponseParser
    /// Parses the status line and the header of the final HTTP response from data that arrives in
    /// pieces of any size. An interim `100 Continue` is dropped, and the response after it is parsed.
    /// For the same bytes, it gives the same response and throws the same exceptions as
    /// `HTTPClientSession::receiveResponse`, which calls `HTTPResponse::read` until the status is
    /// not `100`. It keeps its state between the pieces, so the same parser serves a caller that
    /// reads a socket without blocking and continues later, and a caller that blocks until the
    /// header is complete.
    ///
    /// After an exception, the parser must be reset before it is used again.
    {
    public:
        explicit HTTPResponseParser(const HTTPHeaderLimits & limits = {});

        void reset();
        /// Starts a new response with the same limits.

        void reset(const HTTPHeaderLimits & limits);
        /// Starts a new response with other limits.

        std::size_t feed(const char * data, std::size_t size);
        /// Parses the bytes and returns how many of them belong to the header: all of them until
        /// the header of the final response is complete, and none after it, because the rest
        /// belongs to the body.
        /// Throws `MessageException` as soon as the header breaks the syntax or the limits, also
        /// before the header ends.

        void feedEof();
        /// Reports the end of the data. Completes the header as `HTTPResponse::read` does at the
        /// end of its stream, or throws: `NoMessageException` when no byte of the final response
        /// arrived, also after a `100 Continue`, and `MessageException` when the status line or a
        /// field name is not complete.

        bool statusLineComplete() const { return _statusLineComplete; }
        /// Whether `response` has the version, the status and the reason.

        bool complete() const { return _state == State::Done; }
        /// Whether `response` has the whole header of the final response.

        HTTPResponse & response() { return _response; }
        const HTTPResponse & response() const { return _response; }

        const HTTPHeaderLimits & limits() const { return _limits; }

    private:
        enum class State
        {
            Start, /// -> LeadingSpace
            LeadingSpace, /// -> LeadingSpace, Version
            Version, /// -> Version, SpaceBeforeStatus
            SpaceBeforeStatus, /// -> SpaceBeforeStatus, Status
            Status, /// -> Status, SpaceBeforeReason
            SpaceBeforeReason, /// -> SpaceBeforeReason, Reason
            Reason, /// -> Reason, StatusLineCR, LineStart
            StatusLineCR, /// -> LineStart
            LineStart, /// -> Name, FinalLine, Done, Start (after a `100 Continue`)
            Name, /// -> Name, SpaceBeforeValue, LineStart (a line without a colon)
            SpaceBeforeValue, /// -> SpaceBeforeValue, Value
            Value, /// -> Value, ValueCR, AfterLine
            ValueCR, /// -> AfterLine
            AfterLine, /// -> FoldValue, LineStart
            FoldValue, /// -> FoldValue, FoldCR, AfterLine
            FoldCR, /// -> AfterLine
            FinalLine, /// -> FinalLine, Done, Start (after a `100 Continue`)
            Done, /// -> none
        };

        bool step(char ch);
        /// Processes one byte. Returns false when the byte only moved the parser to another state,
        /// and must be processed again in it.

        void finishStatusLine();
        void finishField();
        /// Ends the header: drops a `100 Continue` and starts the next response, or completes the parse.
        void finishHeader();

        HTTPHeaderLimits _limits;
        HTTPResponse _response;
        State _state = State::Start;
        bool _statusLineComplete = false;
        int _fields = 0;
        std::string _version;
        std::string _status;
        std::string _reason;
        std::string _name;
        std::string _value;
    };


}
} // namespace Poco::Net


#endif // Net_HTTPResponseParser_INCLUDED
