#include <Core/Mongo/Wire/OpReply.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace MongoProtocol
{

OpReply::OpReply(
        Header header_,
        UInt32 flags_, 
        UInt64 cursor_id_,
        UInt32 starting_from_, 
        UInt32 number_returned_, 
        const std::vector<Document> & documents_)
    : header(header_)
    , flags(flags_)
    , cursor_id(cursor_id_)
    , starting_from(starting_from_)
    , number_returned(number_returned_)
    , documents(documents_)
{
}

void OpReply::serialize(WriteBuffer& out) const
{
    header.serialize(out);
    writeBinaryLittleEndian(flags, out);
    writeBinaryLittleEndian(cursor_id, out);
    writeBinaryLittleEndian(starting_from, out);
    writeBinaryLittleEndian(number_returned, out);
    if (documents.size() != 1) 
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serializing documents with size more than 1");
    }
}    

Int32 OpReply::size() const
{
    return 16 + 4 + 8 + 4 + 4 + documents[0].size();
}

}

}
