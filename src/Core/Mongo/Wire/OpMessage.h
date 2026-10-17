#pragma once

#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>
#include <base/types.h>

namespace DB::MongoProtocol
{

struct OpMessageSection : public FrontMessage, BackendMessage
{
    UInt8 kind = 0;
    String identifier;
    std::vector<Document> documents;

    OpMessageSection() = default;
    OpMessageSection(UInt8 kind_, const std::vector<Document> & documents_);
    OpMessageSection(const OpMessageSection & other);
    OpMessageSection(OpMessageSection && other) noexcept;

    void deserialize(ReadBuffer & in) override;
    /// Reads the section, counting its documents against `remaining_documents`, the number of
    /// documents the rest of the message may still carry. The budget is checked before each
    /// document is read, so a message over it is refused before its documents are allocated.
    void deserialize(ReadBuffer & in, size_t & remaining_documents);

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override;
};

struct OpMessage : public virtual FrontMessage, BackendMessage
{
    Header header;
    UInt32 flags = 0;
    std::vector<OpMessageSection> sections;

    OpMessage() = default;
    explicit OpMessage(UInt32 flags_, UInt8 kind_, const std::vector<Document> & documents_);

    void deserialize(ReadBuffer & in) override;
    void serialize(WriteBuffer & out) const override;

    Int32 size() const override;
};

}
