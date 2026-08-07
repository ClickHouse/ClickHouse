#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/Statistics/Statistics.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class IDataPartStorage;
struct WriteSettings;
struct MergeTreeDataPartChecksums;
class WriteBufferFromFileBase;

class ICompressionCodec;
using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;

using WrittenFiles = std::vector<std::unique_ptr<WriteBufferFromFileBase>>;

/// Both name each file after the column's id in `part_columns` (see getColumnIdInPart).

/// Serialize statistics into a single packed archive file (statistics.packed).
std::unique_ptr<WriteBufferFromFileBase> serializeStatisticsPacked(
    IDataPartStorage & data_part_storage,
    MergeTreeDataPartChecksums & out_checksums,
    const ColumnsStatistics & statistics,
    const NamesAndTypesList & part_columns,
    const CompressionCodecPtr & compression_codec,
    const WriteSettings & write_settings);

/// Serialize statistics as separate compressed files (column_name.stats each).
WrittenFiles serializeStatisticsWide(
    IDataPartStorage & data_part_storage,
    MergeTreeDataPartChecksums & out_checksums,
    const ColumnsStatistics & statistics,
    const NamesAndTypesList & part_columns,
    const CompressionCodecPtr & compression_codec,
    const WriteSettings & write_settings);

}
