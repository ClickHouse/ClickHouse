#pragma once

#include <cassert>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>
#include "../BSON/BSONWriter.h"
#include "../BSON/Document.h"
#include "RequestMessage.h"


namespace DB
{
namespace MongoDB
{

class OpReply : Message
{
public:
    explicit OpReply(const MessageHeader & header_) : Message(header_) { }

    void send(WriteBuffer & writer);

    OpReply & setResponseFlags(Int32 response_flags_);

    OpReply & setCursorId(Int64 cursor_id_);

    OpReply & setStartingFrom(Int32 starting_from_);

    OpReply & setNumberReturned(Int32 number_returned_);

    OpReply & addDocument(BSON::Document::Ptr document);

protected:
    Int32 getLength() const;
    void writeContent(WriteBuffer & writer);

private:
    Int32 response_flags;
    Int64 cursor_id;
    Int32 starting_from;
    Int32 number_returned;
    BSON::Document::Vector documents;
};


// inline functions
inline OpReply & OpReply::setCursorId(Int64 cursor_id_)
{
    this->cursor_id = cursor_id_;
    return *this;
}

inline OpReply & OpReply::setStartingFrom(Int32 starting_from_)
{
    this->starting_from = starting_from_;
    return *this;
}

inline OpReply & OpReply::setNumberReturned(Int32 number_returned_)
{
    this->number_returned = number_returned_;
    return *this;
}

}
}
