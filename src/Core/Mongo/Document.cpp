#include <Core/Mongo/Document.h>

#include <bson/bson.h>
#include <base/unaligned.h>
#include <Common/Exception.h>
#include <Core/Mongo/MongoProtocol.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace MongoProtocol
{

Document::Document(bson_t * bson_doc_) : bson_doc(bson_doc_)
{
    if (!bson_doc_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Trying to construct a Mongo document from a null bson");
    doc_size = bson_doc_->len;
    document = String(reinterpret_cast<const char *>(bson_get_data(bson_doc_)), doc_size);
}

Document::Document(const String & json)
{
    bson_error_t error;
    bson_doc = bson_new_from_json(reinterpret_cast<const uint8_t *>(json.c_str()), json.size(), &error);
    if (!bson_doc)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not create bson from {}", json);
    doc_size = bson_doc->len;
    document = String(reinterpret_cast<const char *>(bson_get_data(bson_doc)), doc_size);
}

void Document::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(doc_size, in);

    /// The size is the first field of an unvalidated BSON document taken from the wire.
    /// It must be checked before it is used for an allocation: a too small value would
    /// underflow the subtraction below, and a too large one would either allocate an
    /// arbitrary amount of memory or read past the end of the current message.
    if (doc_size < MIN_DOCUMENT_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Invalid BSON document size {}, it must be at least {}", doc_size, MIN_DOCUMENT_SIZE);

    /// A message may be larger than one document - it carries a header and several sections - so
    /// the limit of a message does not bound a document. `maxBsonObjectSize` of the handshake
    /// promises the client that a document of more than `MAX_BSON_OBJECT_SIZE` bytes is refused,
    /// and the same limit bounds the documents this server produces, so it is enforced here
    /// rather than letting an oversized document through to a handler.
    if (doc_size > MAX_BSON_OBJECT_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid BSON document size {}: it is larger than the maximum of {} bytes",
            doc_size,
            MAX_BSON_OBJECT_SIZE);

    const size_t rest_of_document = doc_size - sizeof(doc_size);
    if (rest_of_document > in.available())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid BSON document size {}: it does not fit into the remaining {} bytes of the message",
            doc_size,
            in.available() + sizeof(doc_size));

    document.resize(doc_size);
    unalignedStoreLittleEndian<UInt32>(document.data(), doc_size);
    in.readStrict(document.data() + sizeof(doc_size), rest_of_document);

    if (bson_doc)
        bson_destroy(bson_doc);
    bson_doc = bson_new_from_data(reinterpret_cast<const uint8_t *>(document.data()), document.size());
    if (!bson_doc)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Malformed BSON document of size {}", doc_size);
}

void Document::serialize(WriteBuffer & out) const
{
    out.write(document.data(), document.size());
}

std::vector<String> Document::getDocumentKeys() const
{
    if (!bson_doc)
    {
        bson_doc = bson_new_from_data(reinterpret_cast<const uint8_t *>(document.data()), document.size());
        if (!bson_doc)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect bson: can not parse the document");
    }

    std::vector<String> result;
    bson_iter_t iter;

    if (!bson_iter_init(&iter, bson_doc))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect bson: can not iterate through keys");

    while (bson_iter_next(&iter))
        result.push_back(bson_iter_key(&iter));
    return result;
}

rapidjson::Document Document::getRapidJSONRepresentation() const
{
    String json = getJSON();
    rapidjson::Document json_doc;
    if (json_doc.Parse(json.c_str()).HasParseError())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect bson: can not convert the document to JSON");
    return json_doc;
}

String Document::getJSON() const
{
    if (!bson_doc)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Trying to serialize an empty Mongo document");

    char * json_str = bson_as_legacy_extended_json(bson_doc, nullptr);
    if (!json_str)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect bson: can not convert the document to JSON");

    String result(json_str);
    bson_free(json_str);
    return result;
}

Document::~Document()
{
    if (bson_doc)
        bson_destroy(bson_doc);
}

}

}
