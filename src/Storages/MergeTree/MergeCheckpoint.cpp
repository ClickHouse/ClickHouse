#include <Storages/MergeTree/MergeCheckpoint.h>

#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/NamesAndTypes.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreePartMinMaxIndex.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>

#include <filesystem>

namespace ProfileEvents
{
    extern const Event MergeCheckpointsSaved;
    extern const Event MergeCheckpointSaveMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename T>
T readBinaryValue(ReadBuffer & in)
{
    T value{};
    readBinary(value, in);
    return value;
}

}

std::expected<void, String> MergeCheckpointIdentity::match(const MergeCheckpointIdentity & other) const
{
    /// TODO(Diskein): replace with `boost::pfr::for_each_field_with_name`, which the static analyzer of clang-tidy 22 fails on.
#define MATCH_FIELD(field) \
    if ((field) != other.field) \
        return std::unexpected(String(#field));

    MATCH_FIELD(result_part)
    MATCH_FIELD(source_parts)
    MATCH_FIELD(source_checksums)
    MATCH_FIELD(metadata_version)
    MATCH_FIELD(codec)
    MATCH_FIELD(marks_extension)
    MATCH_FIELD(storage_type)
    MATCH_FIELD(serialization_settings)
    MATCH_FIELD(algorithm)
    MATCH_FIELD(merging_columns)
    MATCH_FIELD(gathering_columns)
    MATCH_FIELD(minmax_columns)
    MATCH_FIELD(statistics)
    MATCH_FIELD(vertical_lightweight_delete)

#undef MATCH_FIELD
    return {};
}

NamesAndTypesList MergeCheckpointIdentity::getColumns() const
{
    auto columns = merging_columns;
    columns.insert(columns.end(), gathering_columns.begin(), gathering_columns.end());
    return columns;
}

void MergeCheckpointIdentity::write(WriteBuffer & out) const
{
    writeString("MergeCheckpointIdentity", out);
    writeBinary(result_part, out);
    writeBinary(source_parts, out);
    writeBinary(source_checksums, out);
    writeBinary(metadata_version, out);
    writeBinary(codec, out);
    writeBinary(marks_extension, out);
    writeBinary(storage_type.toString(), out);
    writeBinary(serialization_settings, out);
    writeBinary(static_cast<UInt8>(algorithm), out);
    writeBinary(merging_columns.toString(), out);
    writeBinary(gathering_columns.toString(), out);
    writeBinary(minmax_columns.toString(), out);
    writeBinary(statistics, out);
    writeBinary(vertical_lightweight_delete, out);
}

MergeCheckpointIdentity MergeCheckpointIdentity::read(ReadBuffer & in)
{
    assertString("MergeCheckpointIdentity", in);
    MergeCheckpointIdentity result;
    readBinary(result.result_part, in);
    readBinary(result.source_parts, in);
    readBinary(result.source_checksums, in);
    readBinary(result.metadata_version, in);
    readBinary(result.codec, in);
    readBinary(result.marks_extension, in);
    result.storage_type.fromString(readBinaryValue<String>(in));
    readBinary(result.serialization_settings, in);
    result.algorithm = static_cast<MergeAlgorithm>(readBinaryValue<UInt8>(in));
    result.merging_columns = NamesAndTypesList::parse(readBinaryValue<String>(in));
    result.gathering_columns = NamesAndTypesList::parse(readBinaryValue<String>(in));
    result.minmax_columns = NamesAndTypesList::parse(readBinaryValue<String>(in));
    readBinary(result.statistics, in);
    readBinary(result.vertical_lightweight_delete, in);
    return result;
}

void MergeCheckpointState::write(WriteBuffer & out) const
{
    writeString("MergeCheckpointState", out);
    writeBinary(UInt64{1}, out);
    identity.write(out);
    writeBinary(completed_columns, out);
    writeBinary(rows_sources_size, out);
    writeIndexGranularity(granularity, out);
    writeBinary(checksums.getSerializedString(), out);
    writeBinary(columns_substreams.toString(), out);
    WriteBufferFromOwnString infos;
    serialization_infos.writeJSON(infos);
    writeBinary(infos.str(), out);
    for (const auto & column : identity.getColumns())
    {
        const auto it = statistics.find(column.name);
        writeBinary(it != statistics.end(), out);
        if (it != statistics.end())
            it->second->serialize(out);
    }
    writeBinary(minmax_index.has_value(), out);
    if (minmax_index)
    {
        size_t i = 0;
        for (const auto & column : identity.minmax_columns)
            MergeTreePartMinMaxIndex::writeRange(minmax_index->at(i++), *column.type, out);
    }
}

MergeCheckpointState MergeCheckpointState::read(ReadBuffer & in)
{
    assertString("MergeCheckpointState", in);
    if (const auto version = readBinaryValue<UInt64>(in); version != 1)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unsupported merge checkpoint version {}", version);

    MergeCheckpointState result;
    result.identity = MergeCheckpointIdentity::read(in);
    readBinary(result.completed_columns, in);
    readBinary(result.rows_sources_size, in);
    result.granularity = readIndexGranularity(in);
    result.checksums = MergeTreeDataPartChecksums::deserializeFrom(readBinaryValue<String>(in));
    /// Named: the buffer reads from the string.
    const auto substreams = readBinaryValue<String>(in);
    ReadBufferFromString substreams_in(substreams);
    result.columns_substreams.readText(substreams_in);
    const auto columns = result.identity.getColumns();
    result.serialization_infos = SerializationInfoByName::readJSONFromString(columns, readBinaryValue<String>(in));
    for (const auto & column : columns)
    {
        if (!readBinaryValue<bool>(in))
            continue;
        auto column_statistics = ColumnStatistics::deserialize(in, column.type);
        if (!column_statistics)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot read the statistics of column {} from a merge checkpoint", column.name);
        result.statistics.emplace(column.name, std::move(column_statistics));
    }
    if (auto has_minmax_index = readBinaryValue<bool>(in); has_minmax_index)
    {
        result.minmax_index.emplace();
        for (const auto & column : result.identity.minmax_columns)
            result.minmax_index->push_back(MergeTreePartMinMaxIndex::readRange(*column.type, in));
    }
    assertEOF(in);
    return result;
}

void MergeCheckpoint::save(IDataPartStorage & part, const MergeCheckpointState & state)
{
    ProfileEventTimeIncrement<Time::Microseconds> watch(ProfileEvents::MergeCheckpointSaveMicroseconds);

    /// Before the rename, which precommits a blob-list part.
    {
        auto out = part.writeFile(STATE_FILE_NAME, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        CompressedWriteBuffer compressed(*out);
        state.write(compressed);
        compressed.finalize();
        out->finalize();
    }

    if (!isSaved())
    {
        const std::filesystem::path directory = getDirectory();
        part.rename(
            directory.parent_path(),
            directory.filename(),
            /*log=*/nullptr,
            /*remove_new_dir_if_exists=*/false,
            /*fsync_part_dir=*/false);
    }

    if (part.getRelativePath() != (std::filesystem::path(getDirectory()) / "").string())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Part {} is not built in merge checkpoint directory {}", part.getRelativePath(), getDirectory());

    commit(part);

    ProfileEvents::increment(ProfileEvents::MergeCheckpointsSaved);
}

void MergeCheckpointState::checkFiles(const IDataPartStorage & part) const
{
    if (!part.existsFile(MergeCheckpoint::ROWS_SOURCES_FILE_NAME))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Merge checkpoint in {} has no rows_sources", part.getRelativePath());

    if (part.getFileSize(MergeCheckpoint::ROWS_SOURCES_FILE_NAME) != rows_sources_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA, "Merge checkpoint file {} has an unexpected size", MergeCheckpoint::ROWS_SOURCES_FILE_NAME);

    for (const auto & [name, checksum] : checksums.files)
        checksum.checkSize(part, name);
}

MergeCheckpointState MergeCheckpoint::readState(const IDataPartStorage & part)
{
    CompressedReadBufferFromFile in(part.readFile(STATE_FILE_NAME, ReadSettings{}, {}));
    return MergeCheckpointState::read(in);
}

}
