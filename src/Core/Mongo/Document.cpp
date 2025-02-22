#include "Document.h"

#include <iostream>
#include <bson/bson.h>
#include "Common/Exception.h"
#include "Core/Mongo/MongoProtocol.h"
#include "IO/WriteBufferFromString.h"
#include "IO/WriteHelpers.h"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace MongoProtocol
{

std::string bsonToJson(const std::string & bsonData)
{
    bson_t b;

    if (!bson_init_static(&b, reinterpret_cast<const uint8_t *>(bsonData.data()), bsonData.size()))
    {
        throw std::runtime_error("Failed to initialize BSON data");
    }

    char * json_str = bson_as_json(&b, nullptr);
    if (!json_str)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect bson");
    }

    std::string json(json_str);
    bson_free(json_str);
    return json;
}


Document::Document(bson_t * bson_doc_, bool is_message_query_) : bson_doc(bson_doc_), is_message_query(is_message_query_)
{
    doc_size = bson_doc_->len;
    document = String(reinterpret_cast<const char *>(bson_get_data(bson_doc_)), doc_size);
    std::cerr << "document " << bsonToJson(document) << ' ' << document.size() << '\n';
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

    document.resize(doc_size - sizeof(doc_size));
    in.readStrict(document.data(), doc_size - sizeof(doc_size));
    WriteBufferFromOwnString subbuffer;
    writeBinaryLittleEndian(doc_size, subbuffer);
    document = subbuffer.str() + document;

    bson_doc = bson_new_from_data(reinterpret_cast<const uint8_t *>(document.data()), document.size());
    std::cerr << "parsed doc " << bson_as_json(bson_doc, nullptr) << '\n';
}

void Document::serialize(WriteBuffer & out) const
{
    //if (!is_message_query)
    //    writeBinaryLittleEndian(doc_size, out);
    out.write(document.data(), document.size());
    std::cerr << "serialize doc " << is_message_query << '\n';
}

std::vector<String> Document::getDocumentKeys() const
{
    std::cerr << "getDocumentKeys " << '\n';
    if (!bson_doc)
    {
        bson_doc = bson_new_from_data(reinterpret_cast<const uint8_t *>(document.data()), document.size());
        //std::cerr << "print bson " << bson_as_json(bson_doc, nullptr) << '\n';
    }
    std::cerr << "break point 1\n";
    std::vector<String> result;
    bson_iter_t iter;
    const char * key;

    if (!bson_iter_init(&iter, bson_doc))
    {
        std::cerr << "fuck " << '\n';
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect bson : can not iterate through keys");
    }
    std::cerr << "start iter\n";
    while (bson_iter_next(&iter))
    {
        key = bson_iter_key(&iter);
        std::cerr << "iter key " << key << '\n';
        result.push_back(key);
    }
    return result;
}

}

}
