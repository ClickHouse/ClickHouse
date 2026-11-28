#include <Core/Mongo/Wire/OpMessage.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int NOT_IMPLEMENTED;
}

namespace DB::MongoProtocol
{

OpMessageSection::OpMessageSection(OpMessageSection && other) noexcept
    : kind(other.kind), identifier(std::move(other.identifier)), documents(std::move(other.documents))
{
}

OpMessageSection::OpMessageSection(UInt8 kind_, const std::vector<Document> & documents_) : kind(kind_), documents(documents_)
{
}

OpMessageSection::OpMessageSection(const OpMessageSection & other)
    : kind(other.kind), identifier(other.identifier), documents(other.documents)
{
}

void OpMessageSection::serialize(WriteBuffer & out) const
{
    writeBinaryLittleEndian(kind, out);
    for (const auto & doc : documents)
    {
        doc.serialize(out);
    }
}

Int32 OpMessageSection::size() const
{
    Int32 result = sizeof(UInt8);
    for (const auto & doc : documents)
    {
        result += static_cast<Int32>(doc.getDoc().size());
    }
    return result;
}

void OpMessageSection::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(kind, in);
    if (kind == 0)
    {
        /// Body section: exactly one document.
        Document doc;
        doc.deserialize(in);
        documents.push_back(std::move(doc));
    }
    else if (kind == 1)
    {
        /// Document sequence section: its own size, a NUL terminated identifier and then
        /// documents filling the rest of the section.
        UInt32 size_section = 0;
        const size_t section_start = in.count();
        readBinaryLittleEndian(size_section, in);

        if (size_section < sizeof(size_section) + 1 || size_section - sizeof(size_section) > in.available())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Mongo document sequence section of size {}", size_section);

        readNullTerminated(identifier, in);

        const size_t section_end = section_start + size_section;
        if (in.count() > section_end)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Invalid Mongo document sequence section: the identifier does not fit into it");

        while (in.count() < section_end)
        {
            /// The bound is checked before a document is read: an empty document takes five bytes,
            /// so a frame could otherwise make millions of them before any later check.
            if (documents.size() >= max_documents)
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "The document sequences of a Mongo message hold more than {} documents",
                    MAX_WRITE_BATCH_SIZE);
            Document doc;
            doc.deserialize(in);
            documents.push_back(std::move(doc));
        }

        if (in.count() != section_end)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Invalid Mongo document sequence section: the documents do not fit into it");
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported Mongo message section kind {}", static_cast<UInt16>(kind));
    }
}

OpMessage::OpMessage(UInt32 flags_, UInt8 kind_, const std::vector<Document> & documents_)
    : flags(flags_), sections(std::vector<OpMessageSection>{OpMessageSection(kind_, documents_)})
{
}

void OpMessage::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(flags, in);

    /// The checksum would be read as one more section, so a message that carries one is rejected
    /// rather than misparsed.
    if (flags & CHECKSUM_PRESENT)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mongo messages with a checksum are not supported");
    if (UInt32 unknown_required_flags = flags & REQUIRED_FLAGS_MASK & ~MORE_TO_COME)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown required flags {} of a Mongo message", unknown_required_flags);

    /// `in` holds exactly the payload of one message, so reading until it is exhausted
    /// can neither consume a part of the next message nor stop in the middle of this one.
    /// A message has exactly one body - the command - and its document sequences together hold at
    /// most `MAX_WRITE_BATCH_SIZE` documents, the most a write command takes, whatever the command
    /// is. Both are checked while the message is read, before it is authenticated.
    bool has_body = false;
    size_t sequence_documents = 0;
    while (!in.eof())
    {
        if (sections.size() >= MAX_SECTIONS)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "A Mongo message has more than {} sections", MAX_SECTIONS);
        OpMessageSection section;
        section.max_documents = MAX_WRITE_BATCH_SIZE - sequence_documents;
        section.deserialize(in);
        if (section.kind == 0)
        {
            if (has_body)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A Mongo message has more than one body section");
            has_body = true;
        }
        else
            sequence_documents += section.documents.size();
        sections.push_back(std::move(section));
    }

    if (!has_body)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo message without a body section");
}

void OpMessage::serialize(WriteBuffer & out) const
{
    header.serialize(out);
    writeBinaryLittleEndian(flags, out);
    for (const auto & section : sections)
    {
        section.serialize(out);
    }
}

Int32 OpMessage::size() const
{
    Int32 result = header.size() + sizeof(flags);
    for (const auto & doc : sections)
    {
        result += doc.size();
    }
    return result;
}

}
