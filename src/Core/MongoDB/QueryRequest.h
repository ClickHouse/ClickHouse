#pragma once

#include "RequestMessage.h"
#include <base/types.h>
#include <Common/BSONParser/Array.h>
#include <IO/ReadBuffer.h>
#include <Poco/SharedPtr.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace MongoDB
{

    class QueryRequest : public RequestMessage
    /// A request to query documents in a MongoDB database
    /// using an OP_QUERY request.
    {
    public:

        using Ptr = Poco::SharedPtr<RequestMessage>;

        enum Flags
        {
            QUERY_DEFAULT = 0,
            /// Do not set any flags.

            QUERY_TAILABLE_CURSOR = 2,
            /// Tailable means cursor is not closed when the last data is retrieved.
            /// Rather, the cursor marks the final object’s position.
            /// You can resume using the cursor later, from where it was located,
            /// if more data were received. Like any "latent cursor", the cursor may
            /// become invalid at some point (CursorNotFound) – for example if the final
            /// object it references were deleted.

            QUERY_SLAVE_OK = 4,
            /// Allow query of replica slave. Normally these return an error except
            /// for namespace "local".

            // QUERY_OPLOG_REPLAY = 8 (internal replication use only - drivers should not implement)

            QUERY_NO_CURSOR_TIMEOUT = 16,
            /// The server normally times out idle cursors after an inactivity period
            /// (10 minutes) to prevent excess memory use. Set this option to prevent that.

            QUERY_AWAIT_DATA = 32,
            /// Use with QUERY_TAILABLECURSOR. If we are at the end of the data, block for
            /// a while rather than returning no data. After a timeout period, we do
            /// return as normal.

            QUERY_EXHAUST = 64,
            /// Stream the data down full blast in multiple "more" packages, on the
            /// assumption that the client will fully read all data queried.
            /// Faster when you are pulling a lot of data and know you want to pull
            /// it all down.
            /// Note: the client is not allowed to not read all the data unless it
            /// closes the connection.

            QUERY_PARTIAL = 128
            /// Get partial results from a mongos if some shards are down
            /// (instead of throwing an error).
        };
        /// Creates a QueryRequest.
        ///
        /// The full collection name is the concatenation of the database
        /// name with the collection name, using a "." for the concatenation. For example,
        /// for the database "foo" and the collection "bar", the full collection name is
        /// "foo.bar".
        explicit QueryRequest(const MessageHeader & header_) : RequestMessage(header_) {}

        ~QueryRequest() override;
        /// Destroys the QueryRequest.

        Flags getFlags() const;
        /// Returns the flags.

        void setFlags(Flags flag);
        /// Set the flags.

        std::string getFullCollectionName() const;
        /// Returns the <db>.<collection> used for this query.

        Int32 getNumberToSkip() const;
        /// Returns the number of documents to skip.

        void setNumberToSkip(Int32 n);
        /// Sets the number of documents to skip.

        Int32 getNumberToReturn() const;
        /// Returns the number of documents to return.

        void setNumberToReturn(Int32 n);
        /// Sets the number of documents to return (limit).
        

        void send(std::ostream & ostr);
        /// Writes the request to stream.

        
        BSON::Document::Ptr getSelector();
        /// Returns the selector document.

        BSON::Document::Ptr getFieldSelector();
        /// Returns the field selector document.

        void read(ReadBuffer& reader) override;

    // protected:
    //     Int32 getLength() const override;
    //     void writeContent(WriteBuffer & write_buffer ) const override;

    private:
        Flags flags;
        String full_collection_name;
        Int32 number_to_skip;
        Int32 number_to_return;
        BSON::Document::Ptr selector;
        BSON::Document::Ptr return_field_selector;
    };

    QueryRequest::~QueryRequest() {}

    //
    // inlines
    //
    inline QueryRequest::Flags QueryRequest::getFlags() const
    {
        return flags;
    }


    inline void QueryRequest::setFlags(QueryRequest::Flags flags_)
    {
        flags = flags_;
    }


    inline std::string QueryRequest::getFullCollectionName() const
    {
        return full_collection_name;
    }


    inline BSON::Document::Ptr QueryRequest::getSelector()
    {
        return selector;
    }


    inline BSON::Document::Ptr QueryRequest::getFieldSelector()
    {
        return return_field_selector;
    }


    inline Int32 QueryRequest::getNumberToSkip() const
    {
        return number_to_skip;
    }


    inline void QueryRequest::setNumberToSkip(Int32 n)
    {
        number_to_skip = n;
    }


    inline Int32 QueryRequest::getNumberToReturn() const
    {
        return number_to_return;
    }


    inline void QueryRequest::setNumberToReturn(Int32 n)
    {
        number_to_return = n;
    }


void QueryRequest::read(ReadBuffer& reader) {
	QueryRequest query(header);
    Int32 message_length = header.getMessageLength();
	Int32 flags_;
	readIntBinary(flags_, reader);
	query.flags = static_cast<Flags>(flags_);
	readNullTerminated(full_collection_name, reader);
	readIntBinary(number_to_skip, reader);
	readIntBinary(number_to_return, reader);

    message_length -= MessageHeader::MSG_HEADER_SIZE + sizeof(flags_) + sizeof(number_to_skip) + sizeof(number_to_return) +
        full_collection_name.length() + sizeof('\0');
    query.selector = new BSON::Document();
    query.return_field_selector = new BSON::Document();
    message_length -= query.selector->read(reader);
    if (message_length != 0) {
        query.return_field_selector->read(reader);
    }
}


// Int32 QueryRequest::getLength() const
// {
// 	// TODO add some kind of checks
// 	Int32 length = sizeof(flags);
// 	length += full_collection_name.size();
// 	length += sizeof(number_to_skip);
// 	length += sizeof(number_to_return);
// 	length += selector->getLength();
// 	length += return_field_selector->getLength();
// 	return length;
// }

// void QueryRequest::writeContent(WriteBuffer& writer) const
// {
// 	writeIntBinary(static_cast<Int32>(flags), writer);
// 	writeNullTerminatedString(full_collection_name, writer);
// 	writeIntBinary(number_to_skip, writer);
// 	writeIntBinary(number_to_return, writer);
// 	selector->write(writer);
// 	if (!return_field_selector->empty())
// 	{
// 		return_field_selector->write(writer);
// 	}
// }


}
} // namespace DB::MongoDB

