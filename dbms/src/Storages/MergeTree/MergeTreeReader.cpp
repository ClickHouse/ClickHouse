#include <DB/DataTypes/IDataType.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/IO/CachedCompressedReadBuffer.h>
#include <DB/IO/CompressedReadBufferFromFile.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Interpreters/evaluateMissingDefaults.h>
#include <DB/Storages/MergeTree/MergeTreeReader.h>
#include <DB/Columns/ColumnNullable.h>


namespace DB
{

namespace
{
	using OffsetColumns = std::map<std::string, ColumnPtr>;

	constexpr auto DATA_FILE_EXTENSION = ".bin";
	constexpr auto NULL_MAP_EXTENSION = ".null";

	bool isNullStream(const std::string & extension)
	{
		return extension == NULL_MAP_EXTENSION;
	}
}

namespace ErrorCodes
{
	extern const int NOT_FOUND_EXPECTED_DATA_PART;
	extern const int MEMORY_LIMIT_EXCEEDED;
}


MergeTreeReader::~MergeTreeReader() = default;


MergeTreeReader::MergeTreeReader(const String & path, /// Путь к куску
	const MergeTreeData::DataPartPtr & data_part, const NamesAndTypesList & columns,
	UncompressedCache * uncompressed_cache, MarkCache * mark_cache, bool save_marks_in_cache,
	MergeTreeData & storage, const MarkRanges & all_mark_ranges,
	size_t aio_threshold, size_t max_read_buffer_size, const ValueSizeMap & avg_value_size_hints,
	const ReadBufferFromFileBase::ProfileCallback & profile_callback,
	clockid_t clock_type)
	: avg_value_size_hints(avg_value_size_hints), path(path), data_part(data_part), columns(columns),
		uncompressed_cache(uncompressed_cache), mark_cache(mark_cache), save_marks_in_cache(save_marks_in_cache), storage(storage),
		all_mark_ranges(all_mark_ranges), aio_threshold(aio_threshold), max_read_buffer_size(max_read_buffer_size)
{
	try
	{
		if (!Poco::File(path).exists())
			throw Exception("Part " + path + " is missing", ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);

		for (const NameAndTypePair & column : columns)
			addStream(column.name, *column.type, all_mark_ranges, profile_callback, clock_type);
	}
	catch (...)
	{
		storage.reportBrokenPart(data_part->name);
		throw;
	}
}


const MergeTreeReader::ValueSizeMap & MergeTreeReader::getAvgValueSizeHints() const
{
	return avg_value_size_hints;
}


void MergeTreeReader::readRange(size_t from_mark, size_t to_mark, Block & res)
{
	try
	{
		size_t max_rows_to_read = (to_mark - from_mark) * storage.index_granularity;

		/// Указатели на столбцы смещений, общие для столбцов из вложенных структур данных
		/// Если append, все значения nullptr, и offset_columns используется только для проверки, что столбец смещений уже прочитан.
		OffsetColumns offset_columns;

		for (const NameAndTypePair & it : columns)
		{
			if (streams.end() == streams.find(it.name))
				continue;

			/// Все столбцы уже есть в блоке. Будем добавлять значения в конец.
			bool append = res.has(it.name);

			ColumnWithTypeAndName column;
			column.name = it.name;
			column.type = it.type;
			if (append)
				column.column = res.getByName(column.name).column;

			bool read_offsets = true;

			const IDataType * observed_type;
			bool is_nullable;

			if (column.type.get()->isNullable())
			{
				const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*column.type);
				observed_type = nullable_type.getNestedType().get();
				is_nullable = true;
			}
			else
			{
				observed_type = column.type.get();
				is_nullable = false;
			}

			/// Для вложенных структур запоминаем указатели на столбцы со смещениями
			if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(observed_type))
			{
				String name = DataTypeNested::extractNestedTableName(column.name);

				if (offset_columns.count(name) == 0)
					offset_columns[name] = append ? nullptr : std::make_shared<ColumnArray::ColumnOffsets_t>();
				else
					read_offsets = false; /// на предыдущих итерациях смещения уже считали вызовом readData

				if (!append)
				{
					column.column = std::make_shared<ColumnArray>(type_arr->getNestedType()->createColumn(), offset_columns[name]);
					if (is_nullable)
						column.column = std::make_shared<ColumnNullable>(column.column, std::make_shared<ColumnUInt8>());
				}
			}
			else if (!append)
				column.column = column.type->createColumn();

			try
			{
				readData(column.name, *column.type, *column.column, from_mark, max_rows_to_read, 0, read_offsets);
			}
			catch (Exception & e)
			{
				/// Более хорошая диагностика.
				e.addMessage("(while reading column " + column.name + ")");
				throw;
			}

			if (!append && column.column->size())
				res.insert(std::move(column));
		}
	}
	catch (Exception & e)
	{
		if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
			storage.reportBrokenPart(data_part->name);

		/// Более хорошая диагностика.
		e.addMessage("(while reading from part " + path + " from mark " + toString(from_mark) + " to " + toString(to_mark) + ")");
		throw;
	}
	catch (...)
	{
		storage.reportBrokenPart(data_part->name);

		throw;
	}
}


void MergeTreeReader::fillMissingColumns(Block & res, const Names & ordered_names, const bool always_reorder)
{
	fillMissingColumnsImpl(res, ordered_names, always_reorder);
}


void MergeTreeReader::fillMissingColumnsAndReorder(Block & res, const Names & ordered_names)
{
	fillMissingColumnsImpl(res, ordered_names, true);
}


MergeTreeReader::Stream::Stream(
	const String & path_prefix_, const String & extension_,
	UncompressedCache * uncompressed_cache,
	MarkCache * mark_cache, bool save_marks_in_cache,
	const MarkRanges & all_mark_ranges, size_t aio_threshold, size_t max_read_buffer_size,
	const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
	: path_prefix(path_prefix_), extension(extension_)
{
	loadMarks(mark_cache, save_marks_in_cache, isNullStream(extension));

	/// Compute the size of the buffer.
	size_t max_mark_range = 0;

	for (size_t i = 0; i < all_mark_ranges.size(); ++i)
	{
		size_t right = all_mark_ranges[i].end;

		/// Если правая граница лежит внутри блока, то его тоже придется читать.
		if (right < (*marks).size() && (*marks)[right].offset_in_decompressed_block > 0)
		{
			while (right < (*marks).size()
				&& (*marks)[right].offset_in_compressed_file
					== (*marks)[all_mark_ranges[i].end].offset_in_compressed_file)
			{
				++right;
			}
		}

		/// Если правее засечек нет, просто используем max_read_buffer_size
		if (right >= (*marks).size()
			|| (right + 1 == (*marks).size()
				&& (*marks)[right].offset_in_compressed_file
					== (*marks)[all_mark_ranges[i].end].offset_in_compressed_file))
		{
			max_mark_range = max_read_buffer_size;
			break;
		}

		max_mark_range = std::max(max_mark_range,
			(*marks)[right].offset_in_compressed_file - (*marks)[all_mark_ranges[i].begin].offset_in_compressed_file);
	}

	size_t buffer_size = std::min(max_read_buffer_size, max_mark_range);

	/// Compute the estimated size of the data to be read.
	size_t estimated_size = 0;
	if (aio_threshold > 0)
	{
		for (const auto & mark_range : all_mark_ranges)
		{
			size_t offset_begin = (*marks)[mark_range.begin].offset_in_compressed_file;

			size_t offset_end;
			if (mark_range.end < (*marks).size())
				offset_end = (*marks)[mark_range.end].offset_in_compressed_file;
			else
				offset_end = Poco::File(path_prefix + extension).getSize();

			if (offset_end > 0)
				estimated_size += offset_end - offset_begin;
		}
	}

	/// Initialize the objects that shall be used to perform read operations.
	if (uncompressed_cache)
	{
		auto buffer = std::make_unique<CachedCompressedReadBuffer>(
			path_prefix + extension, uncompressed_cache, estimated_size, aio_threshold, buffer_size);

		if (profile_callback)
			buffer->setProfileCallback(profile_callback, clock_type);

		cached_buffer = std::move(buffer);
		data_buffer = cached_buffer.get();
	}
	else
	{
		auto buffer = std::make_unique<CompressedReadBufferFromFile>(
			path_prefix + extension, estimated_size, aio_threshold, buffer_size);

		if (profile_callback)
			buffer->setProfileCallback(profile_callback, clock_type);

		non_cached_buffer = std::move(buffer);
		data_buffer = non_cached_buffer.get();
	}
}

std::unique_ptr<MergeTreeReader::Stream> MergeTreeReader::Stream::createEmptyPtr()
{
	std::unique_ptr<Stream> res(new Stream);
	res->is_empty = true;
	return res;
}


void MergeTreeReader::Stream::loadMarks(MarkCache * cache, bool save_in_cache, bool is_null_stream)
{
	std::string path;

	if (is_null_stream)
		path = path_prefix + ".null_mrk";
	else
		path = path_prefix + ".mrk";

	UInt128 key;
	if (cache)
	{
		key = cache->hash(path);
		marks = cache->get(key);
		if (marks)
			return;
	}

	/// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
	TemporarilyDisableMemoryTracker temporarily_disable_memory_tracker;

	size_t file_size = Poco::File(path).getSize();

	if (file_size % sizeof(MarkInCompressedFile) != 0)
		throw Exception("Size of " + path + " file is not divisable by size of MarkInCompressedFile structure.", ErrorCodes::CORRUPTED_DATA);

	size_t num_marks = file_size / sizeof(MarkInCompressedFile);

	marks = std::make_shared<MarksInCompressedFile>(num_marks);

	/// Read directly to marks.
	ReadBufferFromFile buffer(path, file_size, -1, reinterpret_cast<char *>(marks->data()));

	if (buffer.eof() || buffer.buffer().size() != file_size)
		throw Exception("Cannot read all marks from file " + path, ErrorCodes::CANNOT_READ_ALL_DATA);

	if (cache && save_in_cache)
		cache->set(key, marks);
}


void MergeTreeReader::Stream::seekToMark(size_t index)
{
	MarkInCompressedFile mark = (*marks)[index];

	try
	{
		if (cached_buffer)
			cached_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
		if (non_cached_buffer)
			non_cached_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
	}
	catch (Exception & e)
	{
		/// Более хорошая диагностика.
		if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
			e.addMessage("(while seeking to mark " + toString(index)
				+ " of column " + path_prefix + "; offsets are: "
				+ toString(mark.offset_in_compressed_file) + " "
				+ toString(mark.offset_in_decompressed_block) + ")");

		throw;
	}
}


void MergeTreeReader::addStream(const String & name, const IDataType & type, const MarkRanges & all_mark_ranges,
	const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type,
	size_t level)
{
	String escaped_column_name = escapeForFileName(name);

	const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type);
	bool data_file_exists = Poco::File(path + escaped_column_name + DATA_FILE_EXTENSION).exists();
	bool is_column_of_nested_type = type_arr && level == 0 && DataTypeNested::extractNestedTableName(name) != name;

	/** If data file is missing then we will not try to open it.
	  * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
	  * But we should try to load offset data for array columns of Nested subtable (their data will be filled by default value).
	  */
	if (!data_file_exists && !is_column_of_nested_type)
		return;

	if (type.isNullable())
	{
		/// First create the stream that handles the null map of the given column.
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const IDataType & nested_type = *nullable_type.getNestedType();

		std::string filename = name + NULL_MAP_EXTENSION;

		streams.emplace(filename, std::make_unique<Stream>(
			path + escaped_column_name, NULL_MAP_EXTENSION, uncompressed_cache, mark_cache, save_marks_in_cache,
			all_mark_ranges, aio_threshold, max_read_buffer_size, profile_callback, clock_type));

		/// Then create the stream that handles the data of the given column.
		addStream(name, nested_type, all_mark_ranges, profile_callback, clock_type, level);
	}
	/// Для массивов используются отдельные потоки для размеров.
	else if (type_arr)
	{
		String size_name = DataTypeNested::extractNestedTableName(name)
			+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		String escaped_size_name = escapeForFileName(DataTypeNested::extractNestedTableName(name))
			+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		String size_path = path + escaped_size_name + DATA_FILE_EXTENSION;

		/// We don't have neither offsets neither data -> skipping, default values will be filled after
		if (!data_file_exists && !Poco::File(size_path).exists())
			return;

		if (!streams.count(size_name))
			streams.emplace(size_name, std::make_unique<Stream>(
				path + escaped_size_name, DATA_FILE_EXTENSION, uncompressed_cache, mark_cache, save_marks_in_cache,
				all_mark_ranges, aio_threshold, max_read_buffer_size, profile_callback, clock_type));

		if (data_file_exists)
			addStream(name, *type_arr->getNestedType(), all_mark_ranges, profile_callback, clock_type, level + 1);
		else
			streams.emplace(name, Stream::createEmptyPtr());
	}
	else
		streams.emplace(name, std::make_unique<Stream>(
			path + escaped_column_name, DATA_FILE_EXTENSION, uncompressed_cache, mark_cache, save_marks_in_cache,
			all_mark_ranges, aio_threshold, max_read_buffer_size, profile_callback, clock_type));
}


void MergeTreeReader::readData(const String & name, const IDataType & type, IColumn & column,
	size_t from_mark, size_t max_rows_to_read, size_t level, bool read_offsets)
{
	if (type.isNullable())
	{
		/// First read from the null map.
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const IDataType & nested_type = *nullable_type.getNestedType();

		ColumnNullable & nullable_col = static_cast<ColumnNullable &>(column);
		IColumn & nested_col = *nullable_col.getNestedColumn();

		std::string filename = name + NULL_MAP_EXTENSION;

		Stream & stream = *(streams.at(filename));
		stream.seekToMark(from_mark);
		IColumn & col8 = *(nullable_col.getNullMapColumn());
		DataTypeUInt8{}.deserializeBinary(col8, *stream.data_buffer, max_rows_to_read, 0);

		/// Then read data.
		readData(name, nested_type, nested_col, from_mark, max_rows_to_read, level, read_offsets);
	}
	else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		/// Для массивов требуется сначала десериализовать размеры, а потом значения.
		if (read_offsets)
		{
			Stream & stream = *streams[DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level)];
			stream.seekToMark(from_mark);
			type_arr->deserializeOffsets(
				column,
				*stream.data_buffer,
				max_rows_to_read);
		}

		if (column.size())
		{
			ColumnArray & array = typeid_cast<ColumnArray &>(column);
			const size_t required_internal_size = array.getOffsets()[column.size() - 1];

			if (required_internal_size)
			{
				readData(
					name,
					*type_arr->getNestedType(),
					array.getData(),
					from_mark,
					required_internal_size - array.getData().size(),
					level + 1);

				size_t read_internal_size = array.getData().size();

				/** Исправление для ошибочно записанных пустых файлов с данными массива.
					* Такое бывает после ALTER с добавлением новых столбцов во вложенную структуру данных.
					*/
				if (required_internal_size != read_internal_size)
				{
					if (read_internal_size != 0)
						LOG_ERROR((&Logger::get("MergeTreeReader")),
							"Internal size of array " + name + " doesn't match offsets: corrupted data, filling with default values.");

					array.getDataPtr() = dynamic_cast<IColumnConst &>(
						*type_arr->getNestedType()->createConstColumn(
							required_internal_size,
							type_arr->getNestedType()->getDefault())).convertToFullColumn();

					/** NOTE Можно было бы занулять этот столбец, чтобы он не добавлялся в блок,
					  *  а впоследствии создавался с более правильными (из определения таблицы) значениями по-умолчанию.
					  */
				}
			}
		}
	}
	else
	{
		Stream & stream = *streams[name];

		/// It means that data column of array column will be empty, and it will be replaced by const data column
		if (stream.isEmpty())
			return;

		double & avg_value_size_hint = avg_value_size_hints[name];
		stream.seekToMark(from_mark);
		type.deserializeBinary(column, *stream.data_buffer, max_rows_to_read, avg_value_size_hint);

		/// Вычисление подсказки о среднем размере значения.
		size_t column_size = column.size();
		if (column_size)
		{
			double current_avg_value_size = static_cast<double>(column.byteSize()) / column_size;

			/// Эвристика, чтобы при изменениях, значение avg_value_size_hint быстро росло, но медленно уменьшалось.
			if (current_avg_value_size > avg_value_size_hint)
				avg_value_size_hint = current_avg_value_size;
			else if (current_avg_value_size * 2 < avg_value_size_hint)
				avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
		}
	}
}


void MergeTreeReader::fillMissingColumnsImpl(Block & res, const Names & ordered_names, bool always_reorder)
{
	if (!res)
		throw Exception("Empty block passed to fillMissingColumnsImpl", ErrorCodes::LOGICAL_ERROR);

	try
	{
		/** Для недостающих столбцов из вложенной структуры нужно создавать не столбец пустых массивов, а столбец массивов
		  * правильных длин.
		  * TODO: Если для какой-то вложенной структуры были запрошены только отсутствующие столбцы, для них вернутся пустые
		  * массивы, даже если в куске есть смещения для этой вложенной структуры. Это можно исправить.
		  * NOTE: Похожий код есть в Block::addDefaults, но он немного отличается.
		  */

		/// Сначала запомним столбцы смещений для всех массивов в блоке.
		OffsetColumns offset_columns;
		for (size_t i = 0; i < res.columns(); ++i)
		{
			const ColumnWithTypeAndName & column = res.getByPosition(i);

			IColumn * observed_column;
			std::string column_name;
			if (column.column->isNullable())
			{
				ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*(column.column));
				observed_column = nullable_col.getNestedColumn().get();
				column_name = observed_column->getName();
			}
			else
			{
				observed_column = column.column.get();
				column_name = column.name;
			}

			if (const ColumnArray * array = typeid_cast<const ColumnArray *>(observed_column))
			{
				String offsets_name = DataTypeNested::extractNestedTableName(column_name);
				auto & offsets_column = offset_columns[offsets_name];

				/// Если почему-то есть разные столбцы смещений для одной вложенной структуры, то берём непустой.
				if (!offsets_column || offsets_column->empty())
					offsets_column = array->getOffsetsColumn();
			}
		}

		auto should_evaluate_defaults = false;
		auto should_sort = always_reorder;

		for (const auto & requested_column : columns)
		{
			/// insert default values only for columns without default expressions
			if (!res.has(requested_column.name))
			{
				should_sort = true;
				if (storage.column_defaults.count(requested_column.name) != 0)
				{
					should_evaluate_defaults = true;
					continue;
				}

				ColumnWithTypeAndName column_to_add;
				column_to_add.name = requested_column.name;
				column_to_add.type = requested_column.type;

				String offsets_name = DataTypeNested::extractNestedTableName(column_to_add.name);
				if (offset_columns.count(offsets_name))
				{
					ColumnPtr offsets_column = offset_columns[offsets_name];
					DataTypePtr nested_type = typeid_cast<DataTypeArray &>(*column_to_add.type).getNestedType();
					size_t nested_rows = offsets_column->empty() ? 0
						: typeid_cast<ColumnUInt64 &>(*offsets_column).getData().back();

					ColumnPtr nested_column = dynamic_cast<IColumnConst &>(*nested_type->createConstColumn(
						nested_rows, nested_type->getDefault())).convertToFullColumn();

					column_to_add.column = std::make_shared<ColumnArray>(nested_column, offsets_column);
				}
				else
				{
					/** Нужно превратить константный столбец в полноценный, так как в части блоков (из других кусков),
					  * он может быть полноценным (а то интерпретатор может посчитать, что он константный везде).
					  */
					column_to_add.column = dynamic_cast<IColumnConst &>(*column_to_add.type->createConstColumn(
						res.rows(), column_to_add.type->getDefault())).convertToFullColumn();
				}

				res.insert(std::move(column_to_add));
			}
		}

		/// evaluate defaulted columns if necessary
		if (should_evaluate_defaults)
			evaluateMissingDefaults(res, columns, storage.column_defaults, storage.context);

		/// sort columns to ensure consistent order among all blocks
		if (should_sort)
		{
			Block ordered_block;

			for (const auto & name : ordered_names)
				if (res.has(name))
					ordered_block.insert(res.getByName(name));

			std::swap(res, ordered_block);
		}
	}
	catch (Exception & e)
	{
		/// Более хорошая диагностика.
		e.addMessage("(while reading from part " + path + ")");
		throw;
	}
}

}
