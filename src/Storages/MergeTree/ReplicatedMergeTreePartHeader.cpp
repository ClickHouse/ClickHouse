#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Core/NamesAndTypes.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/SipHash.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

static std::array<char, 16> getSipHash(const String & str)
{
    SipHash hash;
    hash.update(str.data(), str.size());
    std::array<char, 16> result;
    hash.get128(result.data());
    return result;
}

ReplicatedMergeTreePartHeader ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(
    const String & columns_znode, const String & checksums_znode)
{
    auto columns_hash = getSipHash(columns_znode);
    auto checksums = MinimalisticDataPartChecksums::deserializeFrom(checksums_znode);
    return ReplicatedMergeTreePartHeader(std::move(columns_hash), std::move(checksums));
}

ReplicatedMergeTreePartHeader ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
    const NamesAndTypesList & columns,
    const MergeTreeDataPartChecksums & full_checksums)
{
    MinimalisticDataPartChecksums checksums;
    checksums.computeTotalChecksums(full_checksums);
    return ReplicatedMergeTreePartHeader(getSipHash(columns.toString()), std::move(checksums));
}

ReplicatedMergeTreePartHeader ReplicatedMergeTreePartHeader::fromColumnsChecksumsBlockID(
    const NamesAndTypesList & columns,
    const MergeTreeDataPartChecksums & full_checksums,
    const String & block_id_)
{
    MinimalisticDataPartChecksums checksums;
    checksums.computeTotalChecksums(full_checksums);
    return ReplicatedMergeTreePartHeader(getSipHash(columns.toString()), std::move(checksums), block_id_);
}

void ReplicatedMergeTreePartHeader::read(ReadBuffer & in)
{
    in >> "part header format version: 1\n";
    in.readStrict(columns_hash.data(), columns_hash.size());
    checksums.deserializeWithoutHeader(in);

    if (!in.eof())
        in >> "block_id: " >> block_id.value() >> "\n";
}

ReplicatedMergeTreePartHeader ReplicatedMergeTreePartHeader::fromString(const String & str)
{
    ReadBufferFromString in(str);
    ReplicatedMergeTreePartHeader result;
    result.read(in);
    return result;
}

void ReplicatedMergeTreePartHeader::write(WriteBuffer & out) const
{
    writeString("part header format version: 1\n", out);
    out.write(columns_hash.data(), columns_hash.size());
    checksums.serializeWithoutHeader(out);
    if (block_id)
        out << "block_id " << block_id.value() << "\n";
}

String ReplicatedMergeTreePartHeader::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

}
