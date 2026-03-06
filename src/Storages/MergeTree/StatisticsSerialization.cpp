#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/StatisticsSerialization.h>
#include <IO/WriteBuffer.h>
#include <IO/PackedFilesWriter.h>
#include <IO/HashingWriteBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Common/escapeForFileName.h>

namespace DB
{

static String getStatisticsFilename(const String & column_name)
{
    /// Note, we cannot use replaceFileNameToHashIfNeeded(), since we do not handle hashes->column names for statistics in getColumnForStatisticsFile()
    return String(STATS_FILE_PREFIX) + escapeForFileName(column_name) + String(STATS_FILE_SUFFIX);
}

std::unique_ptr<WriteBufferFromFileBase> serializeStatisticsPacked(
    IDataPartStorage & data_part_storage,
    MergeTreeDataPartChecksums & out_checksums,
    const ColumnsStatistics & statistics,
    const CompressionCodecPtr & compression_codec,
    const WriteSettings & write_settings)
{
    PackedFilesWriter packed_writer;

    for (const auto & [column_name, stat] : statistics)
    {
        String filename = getStatisticsFilename(column_name);
        auto out = packed_writer.writeFile(filename, write_settings);

        CompressedWriteBuffer compressor(*out, compression_codec, 1024 * 1024);
        stat->serialize(compressor);
        compressor.finalize();
        out->finalize();
    }

    String statistics_filename = String(ColumnsStatistics::FILENAME);
    auto out_packed = data_part_storage.writeFile(statistics_filename, 4096, write_settings);
    HashingWriteBuffer out_hashing_packed(*out_packed);

    packed_writer.finalize(out_hashing_packed);
    out_hashing_packed.finalize();

    out_checksums.files[statistics_filename].file_size = out_hashing_packed.count();
    out_checksums.files[statistics_filename].file_hash = out_hashing_packed.getHash();
    out_packed->preFinalize();

    return out_packed;
}

WrittenFiles serializeStatisticsWide(
    IDataPartStorage & data_part_storage,
    MergeTreeDataPartChecksums & out_checksums,
    const ColumnsStatistics & statistics,
    const CompressionCodecPtr & compression_codec,
    const WriteSettings & write_settings)
{
    WrittenFiles written_files;

    for (const auto & [column_name, stat] : statistics)
    {
        String filename = getStatisticsFilename(column_name);

        /// Buffer chain: plain_file <- plain_hashing <- compressor <- compressed_hashing
        auto plain_file = data_part_storage.writeFile(filename, 4096, write_settings);
        HashingWriteBuffer plain_hashing(*plain_file);
        CompressedWriteBuffer compressor(plain_hashing, compression_codec, 1024 * 1024);
        HashingWriteBuffer compressed_hashing(compressor);

        stat->serialize(compressed_hashing);

        compressed_hashing.finalize();
        compressor.finalize();
        plain_hashing.finalize();

        auto & checksum = out_checksums.files[filename];
        checksum.is_compressed = true;
        checksum.file_size = plain_hashing.count();
        checksum.file_hash = plain_hashing.getHash();
        checksum.uncompressed_size = compressed_hashing.count();
        checksum.uncompressed_hash = compressed_hashing.getHash();

        plain_file->preFinalize();
        written_files.push_back(std::move(plain_file));
    }

    return written_files;
}

}
