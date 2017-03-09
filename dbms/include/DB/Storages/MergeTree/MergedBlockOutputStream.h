#pragma once

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/HashingWriteBuffer.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Columns/ColumnArray.h>


namespace DB
{


class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
	IMergedBlockOutputStream(
		MergeTreeData & storage_,
		size_t min_compress_block_size_,
		size_t max_compress_block_size_,
		CompressionMethod compression_method_,
		size_t aio_threshold_);

protected:
	using OffsetColumns = std::set<std::string>;

	struct ColumnStream
	{
		ColumnStream(
			const String & escaped_column_name_,
			const String & data_path,
			const std::string & data_file_extension_,
			const std::string & marks_path,
			const std::string & marks_file_extension_,
			size_t max_compress_block_size,
			CompressionMethod compression_method,
			size_t estimated_size,
			size_t aio_threshold);

		String escaped_column_name;
		std::string data_file_extension;
		std::string marks_file_extension;

		/// compressed -> compressed_buf -> plain_hashing -> plain_file
		std::unique_ptr<WriteBufferFromFileBase> plain_file;
		HashingWriteBuffer plain_hashing;
		CompressedWriteBuffer compressed_buf;
		HashingWriteBuffer compressed;

		/// marks -> marks_file
		WriteBufferFromFile marks_file;
		HashingWriteBuffer marks;

		void finalize();

		void sync();

		void addToChecksums(MergeTreeData::DataPart::Checksums & checksums);
	};

	using ColumnStreams = std::map<String, std::unique_ptr<ColumnStream>>;

	void addStream(const String & path, const String & name, const IDataType & type, size_t estimated_size,
		size_t level, const String & filename, bool skip_offsets);

	/// Записать данные одного столбца.
	void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns,
		size_t level, bool skip_offsets);

	MergeTreeData & storage;

	ColumnStreams column_streams;

	/// Смещение до первой строчки блока, для которой надо записать индекс.
	size_t index_offset = 0;

	size_t min_compress_block_size;
	size_t max_compress_block_size;

	size_t aio_threshold;

	CompressionMethod compression_method;

private:
	/// Internal version of writeData.
	void writeDataImpl(const String & name, const IDataType & type, const IColumn & column,
		OffsetColumns & offset_columns, size_t level, bool write_array_data, bool skip_offsets);
};


/** Для записи одного куска.
  * Данные относятся к одному месяцу, и пишутся в один кускок.
  */
class MergedBlockOutputStream : public IMergedBlockOutputStream
{
public:
	MergedBlockOutputStream(
		MergeTreeData & storage_,
		String part_path_,
		const NamesAndTypesList & columns_list_,
		CompressionMethod compression_method);

	MergedBlockOutputStream(
		MergeTreeData & storage_,
		String part_path_,
		const NamesAndTypesList & columns_list_,
		CompressionMethod compression_method,
		const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size_,
		size_t aio_threshold_);

	std::string getPartPath() const;

	/// Если данные заранее отсортированы.
	void write(const Block & block) override;

	/** Если данные не отсортированы, но мы заранее вычислили перестановку, после которой они станут сортированными.
	  * Этот метод используется для экономии оперативки, так как не нужно держать одновременно два блока - исходный и отсортированный.
	  */
	void writeWithPermutation(const Block & block, const IColumn::Permutation * permutation);

	void writeSuffix() override;

	MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums(
		const NamesAndTypesList & total_column_list,
		MergeTreeData::DataPart::Checksums * additional_column_checksums = nullptr);

	MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums();

	MergeTreeData::DataPart::Index & getIndex();

	/// Сколько засечек уже записано.
	size_t marksCount();

private:
	void init();

	/** Если задана permutation, то переставляет значения в столбцах при записи.
	  * Это нужно, чтобы не держать целый блок в оперативке для его сортировки.
	  */
	void writeImpl(const Block & block, const IColumn::Permutation * permutation);

private:
	NamesAndTypesList columns_list;
	String part_path;

	size_t marks_count = 0;

	std::unique_ptr<WriteBufferFromFile> index_file_stream;
	std::unique_ptr<HashingWriteBuffer> index_stream;
	MergeTreeData::DataPart::Index index_columns;
};


/// Записывает только те, столбцы, что лежат в block
class MergedColumnOnlyOutputStream : public IMergedBlockOutputStream
{
public:
	MergedColumnOnlyOutputStream(
		MergeTreeData & storage_, String part_path_, bool sync_, CompressionMethod compression_method, bool skip_offsets_);

	void write(const Block & block) override;
	void writeSuffix() override;
	MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums();

private:
	String part_path;

	bool initialized = false;
	bool sync;
	bool skip_offsets;
};

}
