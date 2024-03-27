#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include "Common/BSONParser/Document.h"
#include <Common/logger_useful.h>
#include "RequestMessage.h"


namespace DB
{
namespace MongoDB
{

class OpReply : Message
{
public:
    using Documents = BSON::Array::Ptr;
    explicit OpReply(const MessageHeader & header_) : Message(header_) { documents = new BSON::Array(); }

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
    Documents documents;
};

inline OpReply & OpReply::setResponseFlags(Int32 response_flags_)
{
    this->response_flags = response_flags_;
    return *this;
}

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

OpReply & OpReply::addDocument(BSON::Document::Ptr document)
{
    this->documents->add(document);
    return *this;
}


void OpReply::send(WriteBuffer & writer)
{
    Int32 size = getLength();
    setContentLength(size);
    header.write(writer);
    writeContent(writer);
}


Int32 OpReply::getLength() const
{
    Int32 length = sizeof(response_flags) + sizeof(cursor_id);
    length += sizeof(starting_from) + sizeof(number_returned);
    length += documents->getLength();
    return length;
}

void OpReply::writeContent(WriteBuffer & writer)
{
    writeIntBinary(response_flags, writer);
    writeIntBinary(cursor_id, writer);
    writeIntBinary(starting_from, writer);
    writeIntBinary(number_returned, writer);
    documents->write(writer);
}

}
} // namespace DB::MongoDB
