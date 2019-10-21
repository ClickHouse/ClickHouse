#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{
    IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
        const String & part_path_,
        const MergeTreeData & storage_,
        const NamesAndTypesList & columns_list_,
        const IColumn::Permutation * permutation_,
        const String & marks_file_extension_,
        const CompressionCodecPtr & default_codec_,
        size_t max_compress_block_size_,
        size_t aio_threshold_)
    : part_path(part_path_)
    , storage(storage_)
    , columns_list(columns_list_)
    , permutation(permutation_)
    , marks_file_extension(marks_file_extension_)
    , default_codec(default_codec_)
    , max_compress_block_size(max_compress_block_size_)
    , aio_threshold(aio_threshold_) {}

void IMergeTreeDataPartWriter::ColumnStream::finalize()
{
    compressed.next();
    plain_file->next();
    marks.next();
}

void IMergeTreeDataPartWriter::ColumnStream::sync()
{
    plain_file->sync();
    marks_file.sync();
}

IMergeTreeDataPartWriter::ColumnStream::ColumnStream(
    const String & escaped_column_name_,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    size_t estimated_size_,
    size_t aio_threshold_) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(createWriteBufferFromFileBase(data_path_ + data_file_extension, estimated_size_, aio_threshold_, max_compress_block_size_)),
    plain_hashing(*plain_file), compressed_buf(plain_hashing, compression_codec_), compressed(compressed_buf),
    marks_file(marks_path_ + marks_file_extension, 4096, O_TRUNC | O_CREAT | O_WRONLY), marks(marks_file)
{
}

// void IMergeTreeDataPartWriter::ColumnStream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
// {
//     String name = escaped_column_name;

//     checksums.files[name + data_file_extension].is_compressed = true;
//     checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
//     checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
//     checksums.files[name + data_file_extension].file_size = plain_hashing.count();
//     checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

//     checksums.files[name + marks_file_extension].file_size = marks.count();
//     checksums.files[name + marks_file_extension].file_hash = marks.getHash();
// }

}