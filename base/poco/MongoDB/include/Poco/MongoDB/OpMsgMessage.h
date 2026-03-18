//
// OpMsgMessage.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  OpMsgMessage
//
// Definition of the OpMsgMessage class.
//
// Copyright (c) 2022, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_OpMsgMessage_INCLUDED
#define MongoDB_OpMsgMessage_INCLUDED


#include "Poco/MongoDB/Document.h"
#include "Poco/MongoDB/Message.h"
#include "Poco/MongoDB/MongoDB.h"

#include <string>

namespace Poco
{
namespace MongoDB
{


    class MongoDB_API OpMsgMessage : public Message
    /// This class represents a request/response (OP_MSG) to send requests and receive responses to/from MongoDB.
    {
    public:
        // Constants for most often used MongoDB commands that can be sent using OP_MSG
        // For complete list see: https://www.mongodb.com/docs/manual/reference/command/

        // Query and write
        static const std::string CMD_INSERT;
        static const std::string CMD_DELETE;
        static const std::string CMD_UPDATE;
        static const std::string CMD_FIND;
        static const std::string CMD_FIND_AND_MODIFY;
        static const std::string CMD_GET_MORE;

        // Aggregation
        static const std::string CMD_AGGREGATE;
        static const std::string CMD_COUNT;
        static const std::string CMD_DISTINCT;
        static const std::string CMD_MAP_REDUCE;

        // Replication and administration
        static const std::string CMD_HELLO;
        static const std::string CMD_REPL_SET_GET_STATUS;
        static const std::string CMD_REPL_SET_GET_CONFIG;

        static const std::string CMD_CREATE;
        static const std::string CMD_CREATE_INDEXES;
        static const std::string CMD_DROP;
        static const std::string CMD_DROP_DATABASE;
        static const std::string CMD_KILL_CURSORS;
        static const std::string CMD_LIST_DATABASES;
        static const std::string CMD_LIST_INDEXES;

        // Diagnostic
        static const std::string CMD_BUILD_INFO;
        static const std::string CMD_COLL_STATS;
        static const std::string CMD_DB_STATS;
        static const std::string CMD_HOST_INFO;


        enum Flags : UInt32
        {
            MSG_FLAGS_DEFAULT = 0,

            MSG_CHECKSUM_PRESENT = (1 << 0),

            MSG_MORE_TO_COME = (1 << 1),
            /// Sender will send another message and is not prepared for overlapping messages

            MSG_EXHAUST_ALLOWED = (1 << 16)
            /// Client is prepared for multiple replies (using the moreToCome bit) to this request
        };

        OpMsgMessage();
        /// Creates an OpMsgMessage for response.

        OpMsgMessage(const std::string & databaseName, const std::string & collectionName, UInt32 flags = MSG_FLAGS_DEFAULT);
        /// Creates an OpMsgMessage for requests.

        virtual ~OpMsgMessage();

        const std::string & databaseName() const;

        const std::string & collectionName() const;

        void setCommandName(const std::string & command);
        /// Sets the command name and clears the command document

        void setCursor(Poco::Int64 cursorID, Poco::Int32 batchSize = -1);
        /// Sets the command "getMore" for the cursor id with batch size (if it is not negative).

        const std::string & commandName() const;
        /// Current command name.

        void setAcknowledgedRequest(bool ack);
        /// Set false to create request that does not return response.
        /// It has effect only for commands that write or delete documents.
        /// Default is true (request returns acknowledge response).

        bool acknowledgedRequest() const;

        UInt32 flags() const;

        Document & body();
        /// Access to body document.
        /// Additional query arguments shall be added after setting the command name.

        const Document & body() const;

        Document::Vector & documents();
        /// Documents prepared for request or retrieved in response.

        const Document::Vector & documents() const;
        /// Documents prepared for request or retrieved in response.

        bool responseOk() const;
        /// Reads "ok" status from the response message.

        void clear();
        /// Clears the message.

        void send(std::ostream & ostr);
        /// Writes the request to stream.

        void read(std::istream & istr);
        /// Reads the response from the stream.

    private:
        enum PayloadType : UInt8
        {
            PAYLOAD_TYPE_0 = 0,
            PAYLOAD_TYPE_1 = 1
        };

        std::string _databaseName;
        std::string _collectionName;
        UInt32 _flags{MSG_FLAGS_DEFAULT};
        std::string _commandName;
        bool _acknowledged{true};

        Document _body;
        Document::Vector _documents;
    };


}
} // namespace Poco::MongoDB


#endif // MongoDB_OpMsgMessage_INCLUDED
