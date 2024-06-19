#pragma once

#include <IO/ReadBuffer.h>
#include <base/types.h>
#include <Poco/SharedPtr.h>
#include <Common/logger_useful.h>
#include "../BSON/Array.h"
#include "../BSON/Document.h"
#include "RequestMessage.h"

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
    explicit QueryRequest(const MessageHeader & header_) : RequestMessage(header_) { }

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

    void read(ReadBuffer & reader) override;

    std::string toString() const override;

private:
    Flags flags;
    String full_collection_name;
    Int32 number_to_skip;
    Int32 number_to_return;
    BSON::Document::Ptr selector;
    BSON::Document::Ptr return_field_selector;
};

}
}
