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

/// Statistics filenames are derived from the part's stamped column IDs
/// (`part_columns`), matching the data-stream naming; the logical name is
/// used only for columns absent from the list.

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
