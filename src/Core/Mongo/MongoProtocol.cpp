#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>

#include <iostream>

namespace DB::MongoProtocol
{
Header::Header(const Header & other)
{
    message_length = other.message_length;
    request_id = other.request_id;
    response_to = other.response_to;
    operation_code = other.operation_code;
}


Header & Header::operator=(const Header & right)
{
    if (this == &right)
        return *this;

    message_length = right.message_length;
    request_id = right.request_id;
    response_to = right.response_to;
    operation_code = right.operation_code;
    return *this;
}

void Header::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(message_length, in);
    readBinaryLittleEndian(request_id, in);
    readBinaryLittleEndian(response_to, in);
    readBinaryLittleEndian(operation_code, in);
}

void Header::serialize(WriteBuffer & out) const
{
    writeBinaryLittleEndian(message_length, out);
    writeBinaryLittleEndian(request_id, out);
    writeBinaryLittleEndian(response_to, out);
    writeBinaryLittleEndian(operation_code, out);
}

Int32 Header::size() const
{
    return 16;
}

void OperationQuery::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(flags, in);
    readNullTerminated(full_collection_name, in);
    readBinaryLittleEndian(number_to_skip, in);
    readBinaryLittleEndian(number_to_skip, in);

    readBinaryLittleEndian(size_query, in);
    {
        String bson_size;
        {
            WriteBufferFromString buf_size(bson_size);
            writeBinaryLittleEndian(size_query, buf_size);
        }
        query.resize(size_query - 4);
        auto readed = static_cast<Int32>(in.read(query.data(), size_query - 4));
        if (readed != size_query - 4)
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Can not read query message from transport");
        }
        query = bson_size + query;
    }
}

IsMasterResponse::IsMasterResponse(Int32 request_id_, Int32 /*response_to_*/)
{
    header.request_id = 5;
    header.response_to = request_id_;

    bson_t * doc = bson_new();

    BSON_APPEND_BOOL(doc, "ismaster", true);
    BSON_APPEND_BOOL(doc, "ismaster", true);
    BSON_APPEND_BOOL(doc, "isWritablePrimary", true);
    BSON_APPEND_INT32(doc, "maxBsonObjectSize", 16777216);
    BSON_APPEND_INT32(doc, "maxMessageSizeBytes", 48000000);
    BSON_APPEND_INT32(doc, "maxWriteBatchSize", 100000);
    BSON_APPEND_INT64(doc, "localTime", static_cast<int64_t>(time(nullptr)) * 1000);
    BSON_APPEND_INT32(doc, "logicalSessionTimeoutMinutes", 30);
    BSON_APPEND_INT32(doc, "minWireVersion", 0);
    BSON_APPEND_INT32(doc, "maxWireVersion", 19);
    BSON_APPEND_BOOL(doc, "readOnly", false);
    BSON_APPEND_DOUBLE(doc, "ok", 1.0);

    bson_data = reinterpret_cast<const char *>(bson_get_data(doc));
    bson_len = doc->len;

    header.operation_code = static_cast<Int32>(OperationCode::OP_REPLY);
    header.message_length = bson_len + 16 + 20;
}

void IsMasterResponse::serialize(WriteBuffer & out) const
{
    //std::cerr << "send back " << bsonToJson(std::string(bson_data, bson_len)) << ' ' << bson_len << '\n';
    header.serialize(out);

    UInt32 cursor_id = 0;
    UInt64 starting_from = 0;
    UInt32 number_returned = 1;

    writeBinaryLittleEndian(cursor_id, out);
    writeBinaryLittleEndian(starting_from, out);
    writeBinaryLittleEndian(number_returned, out);

    writeBinaryLittleEndian(bson_len, out);
    out.write(bson_data, bson_len);
}

Int32 IsMasterResponse::size() const
{
    return bson_len + 16 + 20;
}

OpSection::OpSection(OpSection && other) noexcept
{
    kind = other.kind;
    identifier = other.identifier;
    documents = other.documents;
}

void OpSection::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(kind, in);
    if (kind == 0)
    {
        Int32 doc_size;
        String doc;

        readBinaryLittleEndian(doc_size, in);
        doc.resize(doc_size - 4);
        in.readStrict(doc.data(), doc_size - 4);
        documents.push_back(doc);
    }
    else
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Not implemented kind 1 of messaging "
            "https://github.com/fcoury/oxide/blob/c6211fb1c39040001982eca22467c6fafd6ef0ab/src/wire/util.rs#L67");
    }
}

OpMessageResponse::OpMessageResponse(Int32 request_id_, UInt32 flags_)
{
    flags = flags_;
    header.request_id = 6;
    header.response_to = request_id_;

    bson_t * doc = bson_new();

    BSON_APPEND_INT64(doc, "n", 1);
    BSON_APPEND_DOUBLE(doc, "ok", 1.0);

    bson_data = reinterpret_cast<const char *>(bson_get_data(doc));
    bson_len = doc->len;

    header.operation_code = static_cast<Int32>(OperationCode::OP_REPLY);
    header.message_length = bson_len + 16 + 5;
}

void OpMessageResponse::serialize(WriteBuffer & out) const
{
    //std::cerr << "send back " << bsonToJson(std::string(bson_data, bson_len)) << ' ' << bson_len << '\n';
    header.serialize(out);
    writeBinaryLittleEndian(flags, out);
    UInt8 kind = 0;
    writeBinaryLittleEndian(kind, out);

    writeBinaryLittleEndian(bson_len, out);
    out.write(bson_data, bson_len);
}

}
