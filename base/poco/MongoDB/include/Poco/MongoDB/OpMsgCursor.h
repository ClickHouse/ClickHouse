//
// OpMsgCursor.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  OpMsgCursor
//
// Definition of the OpMsgCursor class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_OpMsgCursor_INCLUDED
#define MongoDB_OpMsgCursor_INCLUDED


#include "Poco/MongoDB/Connection.h"
#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/OpMsgMessage.h"

namespace Poco
{
namespace MongoDB
{


    class MongoDB_API OpMsgCursor : public Document
    /// OpMsgCursor is an helper class for querying multiple documents using OpMsgMessage.
    {
    public:
        OpMsgCursor(const std::string & dbname, const std::string & collectionName);
        /// Creates a OpMsgCursor for the given database and collection.

        virtual ~OpMsgCursor();
        /// Destroys the OpMsgCursor.

        void setEmptyFirstBatch(bool empty);
        /// Empty first batch is used to get error response faster with little server processing

        bool emptyFirstBatch() const;

        void setBatchSize(Int32 batchSize);
        /// Set non-default batch size

        Int32 batchSize() const;
        /// Current batch size (zero or negative number indicates default batch size)

        Int64 cursorID() const;

        OpMsgMessage & next(Connection & connection);
        /// Tries to get the next documents. As long as response message has a
        /// cursor ID next can be called to retrieve the next bunch of documents.
        ///
        /// The cursor must be killed (see kill()) when not all documents are needed.

        OpMsgMessage & query();
        /// Returns the associated query.

        void kill(Connection & connection);
        /// Kills the cursor and reset it so that it can be reused.

    private:
        OpMsgMessage _query;
        OpMsgMessage _response;

        bool _emptyFirstBatch{false};
        Int32 _batchSize{-1};
        /// Batch size used in the cursor. Zero or negative value means that default shall be used.

        Int64 _cursorID{0};
    };


    //
    // inlines
    //
    inline OpMsgMessage & OpMsgCursor::query()
    {
        return _query;
    }

    inline Int64 OpMsgCursor::cursorID() const
    {
        return _cursorID;
    }


}
} // namespace Poco::MongoDB


#endif // MongoDB_OpMsgCursor_INCLUDED
