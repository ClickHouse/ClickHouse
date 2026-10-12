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
