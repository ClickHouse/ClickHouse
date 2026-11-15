#pragma once

#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>

namespace DB::MongoProtocol
{

struct OpQuery : public FrontMessage, BackendMessage
{
    Header header;
    Int32 flags = 0;
    String full_collection_name;
    Int32 number_to_skip = 0;
    Int32 number_to_return = 0;
    Document query;

    OpQuery() = default;
    explicit OpQuery(Document && query_) : query(std::move(query_)) { }

    void deserialize(ReadBuffer & in) override;

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override { return header.size() + static_cast<Int32>(query.getDoc().size()) + 20; }
};

}
