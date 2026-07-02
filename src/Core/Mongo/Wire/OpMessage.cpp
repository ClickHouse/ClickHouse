#include "OpMessage.h"
#include <Core/Mongo/Handler.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include "IO/ReadHelpers.h"

namespace DB::MongoProtocol
{

OpMessageSection::OpMessageSection(OpMessageSection && other) noexcept : kind(other.kind), documents(std::move(other.documents))
{
}

OpMessageSection::OpMessageSection(UInt8 kind_, const std::vector<Document> & documents_) : kind(kind_), documents(documents_)
{
}

OpMessageSection::OpMessageSection(const OpMessageSection & other) : kind(other.kind), documents(other.documents)
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
        result += doc.getDoc().size();
    }
    return result;
}

void OpMessageSection::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(kind, in);
    if (kind == 0)
    {
        Document doc;
        doc.deserialize(in);
        documents.push_back(doc);
    }
    else
    {
        UInt32 size_section;
        readBinaryLittleEndian(size_section, in);
        readNullTerminated(identifier, in);

        size_t rest_available = static_cast<size_t>(in.available()) - (size_section - 5 - identifier.size());
        while (in.available() > rest_available)
        {
            Document doc;
            doc.deserialize(in);
            documents.push_back(doc);
        }
    }
}

OpMessage::OpMessage(UInt32 flags_, UInt8 kind_, const std::vector<Document> & documents_)
    : flags(flags_), sections(std::vector<OpMessageSection>{OpMessageSection(kind_, documents_)})
{
}

void OpMessage::deserialize(ReadBuffer & in)
{
    readBinaryLittleEndian(flags, in);
    while (in.available() > 0)
    {
        OpMessageSection section;
        section.deserialize(in);
        sections.push_back(std::move(section));
    }
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
