#pragma once

#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct OpQuery : public FrontMessage, BackendMessage
{
    Header header;
    Int32 flags = 0;
    String full_collection_name;
    Int32 number_to_skip;
    Int32 number_to_return;
    Document query;

    OpQuery() = default;
    explicit OpQuery(Document&& query_)
        : query(query_)
    {
    }

    void deserialize(ReadBuffer & in) override;

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override
    {
        return header.size() + static_cast<Int32>(query.getDoc().size()) + 20;
    }
};

}
