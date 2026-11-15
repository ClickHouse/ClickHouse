#pragma once

#include <Core/Mongo/MongoProtocol.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <bson/bson.h>
#include <bsoncxx/exception/error_code.hpp>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>


namespace DB::MongoProtocol
{

/// Owns a single BSON document, both in its wire representation (`document`) and as a
/// parsed `bson_t` (`bson_doc`). The `bson_t` is owned by this object: it is released in
/// the destructor, copied on copy and stolen on move.
class Document : public FrontMessage, BackendMessage
{
public:
    Document() = default;

    Document(const Document & other)
    {
        doc_size = other.doc_size;
        document = other.document;
        bson_doc = other.bson_doc ? bson_copy(other.bson_doc) : nullptr;
    }

    Document(Document && other) noexcept
    {
        doc_size = other.doc_size;
        document = std::move(other.document);
        bson_doc = other.bson_doc;
        other.bson_doc = nullptr;
    }

    /// Takes ownership of `bson_doc_`, which must have been created by one of the
    /// `bson_new*` functions.
    explicit Document(bson_t * bson_doc_);
    explicit Document(const String & json);

    void deserialize(ReadBuffer & in) override;

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override { return static_cast<Int32>(document.size()); }

    std::vector<String> getDocumentKeys() const;

    String getDoc() const { return document; }

    /// Returns the document as an owning rapidjson value. The returned Document
    /// owns its allocator, so the JSON stays valid after this function returns
    /// (the data must not reference the temporary local document's allocator).
    rapidjson::Document getRapidJSONRepresentation() const;

    bson_t * getBson() const { return bson_doc; }

    String getJSON() const;

    ~Document() override;

private:
    UInt32 doc_size = 0;
    String document;
    mutable bson_t * bson_doc = nullptr;
};

}
