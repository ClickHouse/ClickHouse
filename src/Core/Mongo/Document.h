#pragma once

#include <Core/Mongo/MongoProtocol.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <iostream>
#include <bsoncxx/exception/error_code.hpp>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>


namespace DB::MongoProtocol
{

std::string bsonToJson(const std::string & bsonData);

class Document : public FrontMessage, BackendMessage
{
public:
    Document() = default;
    Document(const Document & other)
    {
        doc_size = other.doc_size;
        document = other.document;
        bson_doc = other.bson_doc;
        is_message_query = other.is_message_query;
    }

    Document(Document && other) noexcept
    {
        doc_size = other.doc_size;
        document = other.document;
        bson_doc = other.bson_doc;
        is_message_query = other.is_message_query;
    }

    explicit Document(bson_t * bson_doc_, bool is_message_query_);
    explicit Document(const String & json);

    void deserialize(ReadBuffer & in) override;

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override { return /*is_message_query ? 0 : sizeof(doc_size) + */ static_cast<Int32>(document.size()); }

    std::vector<String> getDocumentKeys() const;

    String getDoc() const
    {
        std::cerr << "doc size " << document.size() << '\n';
        return document;
    }

    rapidjson::Value getRapidJsonRepresentation() const
    {
        char * json_str = bson_as_json(bson_doc, nullptr);
        rapidjson::Document json_doc;
        json_doc.Parse(json_str);

        rapidjson::Value & root = json_doc;
        return root.GetObject();
    }

    bson_t * getBson() const { return bson_doc; }

    String getJson() const { return bson_as_json(bson_doc, nullptr); }

private:
    UInt32 doc_size;
    String document;
    mutable bson_t * bson_doc = nullptr;
    bool is_message_query = false;
};

}
