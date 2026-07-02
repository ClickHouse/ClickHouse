#pragma once

#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Handler.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>

namespace DB::MongoProtocol
{

struct OpReply : public BackendMessage
{
    Header header;
    UInt32 flags;
    UInt64 cursor_id;
    UInt32 starting_from;
    UInt32 number_returned;
    std::vector<Document> documents;

    explicit OpReply(
        Header header_,
        UInt32 flags_, 
        UInt64 cursor_id_,
        UInt32 starting_from_, 
        UInt32 number_returned_, 
        const std::vector<Document> & documents_);

    void serialize(WriteBuffer& out) const override;

    Int32 size() const override;
};

}
