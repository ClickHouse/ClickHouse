#include <Storages/MergeTree/RangesInDataPart.h>

#include <Core/ProtocolDefines.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <IO/VarInt.h>

template <>
struct fmt::formatter<DB::RangesInDataPartDescription>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::RangesInDataPartDescription & range, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", range.describe());
    }
};

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int UNKNOWN_PROTOCOL;
}


void RangesInDataPartDescription::serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const
{
    info.serialize(out);
    ranges.serialize(out);
    writeVarUInt(rows, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION)
        writeBinary(projection_name, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        writeVarUInt(min_marks_per_task, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_TOTAL_MARKS_IN_PART)
        writeVarUInt(total_marks_in_part, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PART_FINGERPRINT)
    {
        writeVarUInt(part_checksum_low64, out);
        writeVarUInt(part_checksum_high64, out);
        writeVarUInt(static_cast<UInt64>(part_name_identity), out);
    }
}

String RangesInDataPartDescription::describe() const
{
    String result;
    result += fmt::format("{}[{}]", getPartOrProjectionName(), fmt::join(ranges, ","));
    return result;
}

String RangesInDataPartDescription::getPartOrProjectionName() const
{
    if (projection_name.empty())
        return info.getPartNameV1();

    return info.getPartNameV1() + "." + projection_name;
}

void RangesInDataPartDescription::deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version)
{
    info.deserialize(in);
    ranges.deserialize(in);
    readVarUInt(rows, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION)
        readBinary(projection_name, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        readVarUInt(min_marks_per_task, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_TOTAL_MARKS_IN_PART)
        readVarUInt(total_marks_in_part, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PART_FINGERPRINT)
    {
        readVarUInt(part_checksum_low64, in);
        readVarUInt(part_checksum_high64, in);

        UInt64 part_name_identity_value = 0;
        readVarUInt(part_name_identity_value, in);
        if (part_name_identity_value > static_cast<UInt64>(PartNameIdentity::ClusterWide))
            throw Exception(
                ErrorCodes::UNKNOWN_PROTOCOL,
                "Unexpected part name identity value {} in parallel replicas part description",
                part_name_identity_value);
        part_name_identity = static_cast<PartNameIdentity>(part_name_identity_value);
    }
}

void RangesInDataPartsDescription::serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const
{
    writeVarUInt(this->size(), out);
    for (const auto & desc : *this)
        desc.serialize(out, parallel_replicas_protocol_version);
}

String RangesInDataPartsDescription::describe() const
{
    return fmt::format("{} parts: [{}]", this->size(), fmt::join(*this, ", "));
}

void RangesInDataPartsDescription::deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version)
{
    size_t new_size = 0;
    readVarUInt(new_size, in);
    if (new_size > 100'000'000'000)
        throw DB::Exception(DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE, "The size of serialized parts description is suspiciously large: {}", new_size);

    this->resize(new_size);
    for (auto & desc : *this)
        desc.deserialize(in, parallel_replicas_protocol_version);
}

void RangesInDataPartsDescription::merge(const RangesInDataPartsDescription & other)
{
    for (const auto & desc : other)
        this->emplace_back(desc);
}

RangesInDataPart::RangesInDataPart(
    const DataPartPtr & data_part_,
    const DataPartPtr & parent_part_,
    size_t part_index_in_query_,
    size_t part_starting_offset_in_query_,
    const MarkRanges & ranges_,
    const RangesInDataPartReadHints & read_hints_)
    : data_part{data_part_}
    , parent_part{parent_part_}
    , part_index_in_query{part_index_in_query_}
    , part_starting_offset_in_query{part_starting_offset_in_query_}
    , ranges{ranges_}
    , read_hints{read_hints_}
{
}

RangesInDataPart::RangesInDataPart(
    const DataPartPtr & data_part_, const DataPartPtr & parent_part_, size_t part_index_in_query_, size_t part_starting_offset_in_query_)
    : data_part{data_part_}
    , parent_part{parent_part_}
    , part_index_in_query{part_index_in_query_}
    , part_starting_offset_in_query{part_starting_offset_in_query_}
{
    size_t total_marks_count = data_part->index_granularity->getMarksCountWithoutFinal();
    if (total_marks_count)
        ranges.emplace_back(0, total_marks_count);
}

/// Whether a part name of `storage` identifies the same content on every cluster member.
///
/// Two independent guarantees make it so:
///
///   * The engine coordinates block numbers through Keeper (`ReplicatedMergeTree` and descendants),
///     so a part name is globally unique by construction.
///
///   * All of the table's data lives on storage whose metadata is shared by every cluster member -
///     `MetadataStorageType::Plain`, `PlainRewritable`, `StaticWeb`, `WebIndex` and `Keeper`. There
///     every member enumerates literally the same parts, so same-named parts trivially hold the
///     same content even for a plain `MergeTree`. Deriving this from the engine's replication bit
///     alone would misclassify such a deployment as node-local and reject perfectly safe queries.
///
/// `MetadataStorageType::Local` and `Memory` keep metadata per node, so a plain `MergeTree` on them
/// is node-local: two members can each mint an `all_1_1_0` holding different rows.
///
/// Deriving this inspects the table's storage policy, which takes a global lock, so callers that
/// describe many parts of the same table should derive it once and pass it down as a hint.
RangesInDataPartDescription::PartNameIdentity partNameIdentityOf(const MergeTreeData & storage)
{
    using PartNameIdentity = RangesInDataPartDescription::PartNameIdentity;

    if (storage.supportsReplication())
        return PartNameIdentity::ClusterWide;

    const auto disks = storage.getDisks();
    if (disks.empty())
        return PartNameIdentity::NodeLocal;

    for (const auto & disk : disks)
    {
        switch (disk->getDataSourceDescription().metadata_type)
        {
            case MetadataStorageType::Plain:
            case MetadataStorageType::PlainRewritable:
            case MetadataStorageType::StaticWeb:
            case MetadataStorageType::WebIndex:
            case MetadataStorageType::Keeper:
                break;
            case MetadataStorageType::None:
            case MetadataStorageType::Local:
            case MetadataStorageType::Memory:
                return PartNameIdentity::NodeLocal;
        }
    }
    return PartNameIdentity::ClusterWide;
}

RangesInDataPartDescription RangesInDataPart::getDescription(
    std::optional<RangesInDataPartDescription::PartNameIdentity> part_name_identity_hint) const
{
    chassert(!data_part->isProjectionPart() || parent_part);

    /// Content fingerprint of the underlying part. Identifies the actual on-disk data, so two
    /// genuinely-different same-named parts produce different fingerprints (used by
    /// `ParallelReplicasReadingCoordinator` to reject divergent local data even when mark counts
    /// happen to coincide). When `checksums` is empty (rare paths where the file is not loaded),
    /// the fingerprint is left at `(0, 0)` and the coordinator falls back to `total_marks_in_part`.
    UInt64 fingerprint_low64 = 0;
    UInt64 fingerprint_high64 = 0;
    if (!data_part->checksums.empty())
    {
        const auto fingerprint = data_part->checksums.getTotalChecksumUInt128();
        fingerprint_low64 = fingerprint.low64;
        fingerprint_high64 = fingerprint.high64;
    }

    return RangesInDataPartDescription{
        .info = data_part->isProjectionPart() ? parent_part->info : data_part->info,
        .ranges = ranges,
        .rows = getRowsCount(),
        .projection_name = data_part->isProjectionPart() ? data_part->name : "",
        /// Total mark count of the underlying part — invariant across replicas with the same
        /// underlying data and unaffected by per-replica PK or skip-index analysis. Used by
        /// `ParallelReplicasReadingCoordinator` as a cheap sanity check.
        .total_marks_in_part = data_part->index_granularity->getMarksCountWithoutFinal(),
        .part_checksum_low64 = fingerprint_low64,
        .part_checksum_high64 = fingerprint_high64,
        /// Tells the coordinator whether a part name is a content identity here (replicated engines
        /// and shared-metadata storage) or same-named parts must be verified by fingerprint (a plain
        /// `MergeTree` on node-local storage).
        .part_name_identity = part_name_identity_hint.value_or(partNameIdentityOf(data_part->storage)),
    };
}

size_t RangesInDataPart::getMarksCount() const
{
    return ranges.getNumberOfMarks();
}

size_t RangesInDataPart::getRowsCount() const
{
    return data_part->index_granularity->getRowsCountInRanges(ranges);
}

RangesInDataParts::RangesInDataParts(const DataPartsVector & parts)
{
    size_t num_parts = parts.size();
    reserve(num_parts);
    size_t starting_offset = 0;
    for (size_t i = 0; i < num_parts; ++i)
    {
        chassert(!parts[i]->isProjectionPart());
        emplace_back(parts[i], nullptr, i, starting_offset);
        starting_offset += parts[i]->rows_count;
    }
}

RangesInDataPartsDescription RangesInDataParts::getDescriptions() const
{
    RangesInDataPartsDescription result;
    if (empty())
        return result;

    /// Every part here belongs to the same table, so derive the identity class once - it inspects the
    /// storage policy under a global lock, which we do not want to do per part.
    const auto part_name_identity = partNameIdentityOf(front().data_part->storage);
    for (const auto & part : *this)
        result.emplace_back(part.getDescription(part_name_identity));
    return result;
}


size_t RangesInDataParts::getMarksCountAllParts() const
{
    size_t result = 0;
    for (const auto & part : *this)
        result += part.getMarksCount();
    return result;
}

size_t RangesInDataParts::getRowsCountAllParts() const
{
    size_t result = 0;
    for (const auto & part: *this)
        result += part.getRowsCount();
    return result;
}

}
