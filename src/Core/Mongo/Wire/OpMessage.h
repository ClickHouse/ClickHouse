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
    /// The most documents a document sequence section may hold, checked while it is read, see
    /// `OpMessage::deserialize`.
    size_t max_documents = MAX_WRITE_BATCH_SIZE;

    OpMessageSection() = default;
    OpMessageSection(UInt8 kind_, const std::vector<Document> & documents_);
    OpMessageSection(const OpMessageSection & other);
    OpMessageSection(OpMessageSection && other) noexcept;

    void deserialize(ReadBuffer & in) override;

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override;
};

struct OpMessage : public virtual FrontMessage, BackendMessage
{
    /// The flag bits of `OP_MSG`. The low 16 bits are required: a receiver must reject a message
    /// with a required bit it does not know. The high 16 bits are optional and may be ignored.
    static constexpr UInt32 CHECKSUM_PRESENT = 1u << 0;
    static constexpr UInt32 MORE_TO_COME = 1u << 1;
    static constexpr UInt32 REQUIRED_FLAGS_MASK = 0xFFFFu;
    /// The most sections of one message. A command has one body and a driver adds a document
    /// sequence for each of the few arrays of a write command, while an empty section takes a few
    /// bytes of a frame, which would allow millions of them.
    static constexpr size_t MAX_SECTIONS = 64;

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
