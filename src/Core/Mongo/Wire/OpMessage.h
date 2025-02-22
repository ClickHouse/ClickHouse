#pragma once

#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Document.h>
#include <base/types.h>

namespace DB::MongoProtocol
{

struct OpMessageSection : public FrontMessage, BackendMessage
{
    UInt8 kind;
    String identifier;
    std::vector<Document> documents;

    OpMessageSection() = default;
    OpMessageSection(UInt8 kind_, const std::vector<Document> & documents_);
    OpMessageSection(const OpMessageSection& other);
    OpMessageSection(OpMessageSection&& other) noexcept;

    void deserialize(ReadBuffer & in) override;

    void serialize(WriteBuffer& out) const override;

    Int32 size() const override;
};

struct OpMessage : public virtual FrontMessage, BackendMessage
{
    Header header;
    UInt32 flags;
    std::vector<OpMessageSection> sections;

    OpMessage() = default;
    explicit OpMessage(UInt32 flags_, UInt8 kind_, const std::vector<Document>& documents_);

    void deserialize(ReadBuffer & in) override;
    void serialize(WriteBuffer& out) const override;

    Int32 size() const override;
};

}
