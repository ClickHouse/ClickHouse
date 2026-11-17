#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Range.h>
#include <Core/Types.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/Statistics/Statistics.h>

#include <expected>
#include <memory>
#include <optional>
#include <string_view>

namespace DB
{

class IDataPartStorage;
class ReadBuffer;
class WriteBuffer;

/// Everything a merge computes from its inputs. Two attempts with equal identities build the same part.
struct MergeCheckpointIdentity
{
    String result_part;
    Strings source_parts;
    Strings source_checksums;
    Int32 metadata_version = 0;
    String codec;
    String marks_extension;
    MergeTreeDataPartStorageType storage_type;
    /// The hash of the settings of the serialization infos. Table settings can change without a new metadata version.
    UInt64 serialization_settings = 0;
    MergeAlgorithm algorithm = MergeAlgorithm::Undecided;
    NamesAndTypesList merging_columns;
    NamesAndTypesList gathering_columns;
    /// The columns of the minmax index that the merge builds.
    NamesAndTypesList minmax_columns;
    Strings statistics;
    bool vertical_lightweight_delete = false;

    /// Fails with the name of the first field that differs.
    std::expected<void, String> match(const MergeCheckpointIdentity & other) const;

    NamesAndTypesList getColumns() const;

    void write(WriteBuffer & out) const;
    static MergeCheckpointIdentity read(ReadBuffer & in);
};

/// The horizontal stage and the gathered columns completed after it.
struct MergeCheckpointState
{
    MergeCheckpointIdentity identity;
    UInt64 completed_columns = 0;
    UInt64 rows_sources_size = 0;
    MergeTreeIndexGranularityPtr granularity;
    MergeTreeDataPartChecksums checksums;
    ColumnsSubstreams columns_substreams;
    SerializationInfoByName serialization_infos{SerializationInfoSettings{}};
    ColumnsStatistics statistics;
    std::optional<Ranges> minmax_index;

    void write(WriteBuffer & out) const;
    static MergeCheckpointState read(ReadBuffer & in);

    void checkFiles(const IDataPartStorage & part) const;
};

/// A directory where a merge builds its part durably, so that another attempt can continue it. The first save moves
/// the part there, and every save commits the files written so far. The backend fences commits against other attempts
/// and removes stale checkpoints.
class MergeCheckpoint
{
public:
    static constexpr auto STATE_FILE_NAME = "checkpoint.state";
    static constexpr auto ROWS_SOURCES_FILE_NAME = "checkpoint.rows_sources";

    virtual ~MergeCheckpoint() = default;

    virtual String getDirectory() const = 0;
    virtual bool isSaved() const = 0;

    virtual void discard(std::string_view reason) = 0;

    void save(IDataPartStorage & part, const MergeCheckpointState & state);

    static MergeCheckpointState readState(const IDataPartStorage & part);

protected:
    /// Commit `part` fenced against other attempts, and begin its next transaction.
    virtual void commit(IDataPartStorage & part) = 0;
};

using MergeCheckpointPtr = std::shared_ptr<MergeCheckpoint>;

}
