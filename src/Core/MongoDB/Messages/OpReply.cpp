#include "OpReply.h"

namespace DB
{

namespace MongoDB
{

OpReply & OpReply::setResponseFlags(Int32 response_flags_)
{
    this->response_flags = response_flags_;
    return *this;
}


OpReply & OpReply::addDocument(BSON::Document::Ptr document)
{
    this->documents.push_back(document);
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
    length += BSON::BSONWriter::getLength(documents);
    return length;
}

void OpReply::writeContent(WriteBuffer & writer)
{
    writeIntBinary(response_flags, writer);
    writeIntBinary(cursor_id, writer);
    writeIntBinary(starting_from, writer);
    writeIntBinary(number_returned, writer);
    assert(number_returned == static_cast<Int32>(documents.size()));
    BSON::BSONWriter(writer).write(documents);
}

}
}
