#include <boost/bind.hpp>
#include <numeric>

#include <Poco/DirectoryIterator.h>
#include <Poco/NumberParser.h>
#include <Poco/Ext/ScopedTry.h>

#include <Yandex/time2str.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/ConcatBlockInputStream.h>
#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/FilterBlockInputStream.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Interpreters/sortBlock.h>

#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/PkCondition.h>


#define MERGE_TREE_MARK_SIZE (2 * sizeof(size_t))


namespace DB
{

class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	MergeTreeBlockOutputStream(StoragePtr owned_storage) : IBlockOutputStream(owned_storage), storage(dynamic_cast<StorageMergeTree &>(*owned_storage)), flags(O_TRUNC | O_CREAT | O_WRONLY)
	{
	}

	void write(const Block & block)
	{
		storage.check(block, true);

		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();

		size_t rows = block.rows();
		size_t columns = block.columns();
		
		/// Достаём столбец с датой.
		const ColumnUInt16::Container_t & dates =
			dynamic_cast<const ColumnUInt16 &>(*block.getByName(storage.date_column_name).column).getData();

		/// Минимальная и максимальная дата.
		UInt16 min_date = std::numeric_limits<UInt16>::max();
		UInt16 max_date = std::numeric_limits<UInt16>::min();
		for (ColumnUInt16::Container_t::const_iterator it = dates.begin(); it != dates.end(); ++it)
		{
			if (*it < min_date)
				min_date = *it;
			if (*it > max_date)
				max_date = *it;
		}

		/// Разделяем на блоки по месяцам. Для каждого ещё посчитаем минимальную и максимальную дату.
		typedef std::map<UInt16, BlockWithDateInterval> BlocksByMonth;
		BlocksByMonth blocks_by_month;

		UInt16 min_month = date_lut.toFirstDayNumOfMonth(Yandex::DayNum_t(min_date));
		UInt16 max_month = date_lut.toFirstDayNumOfMonth(Yandex::DayNum_t(max_date));

		/// Типичный случай - когда месяц один (ничего разделять не нужно).
		if (min_month == max_month)
			blocks_by_month[min_month] = BlockWithDateInterval(block, min_date, max_date);
		else
		{
			for (size_t i = 0; i < rows; ++i)
			{
				UInt16 month = date_lut.toFirstDayNumOfMonth(Yandex::DayNum_t(dates[i]));
			
				BlockWithDateInterval & block_for_month = blocks_by_month[month];
				if (!block_for_month.block)
					block_for_month.block = block.cloneEmpty();

				if (dates[i] < block_for_month.min_date)
					block_for_month.min_date = dates[i];
				if (dates[i] > block_for_month.max_date)
					block_for_month.max_date = dates[i];
					
				for (size_t j = 0; j < columns; ++j)
					block_for_month.block.getByPosition(j).column->insert((*block.getByPosition(j).column)[i]);
			}
		}

		/// Для каждого месяца.
		for (BlocksByMonth::iterator it = blocks_by_month.begin(); it != blocks_by_month.end(); ++it)
			writePart(it->second.block, it->second.min_date, it->second.max_date);
	}

	BlockOutputStreamPtr clone() { return new MergeTreeBlockOutputStream(owned_storage); }

private:
	StorageMergeTree & storage;

	const int flags;

	struct BlockWithDateInterval
	{
		Block block;
		UInt16 min_date;
		UInt16 max_date;

		BlockWithDateInterval() : min_date(std::numeric_limits<UInt16>::max()), max_date(0) {}
		BlockWithDateInterval(const Block & block_, UInt16 min_date_, UInt16 max_date_)
			: block(block_), min_date(min_date_), max_date(max_date_) {}
	};

	void writePart(Block & block, UInt16 min_date, UInt16 max_date)
	{
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		
		size_t rows = block.rows();
		size_t columns = block.columns();
		UInt64 part_id = storage.increment.get(true);

		String part_name = storage.getPartName(
			Yandex::DayNum_t(min_date), Yandex::DayNum_t(max_date),
			part_id, part_id, 0);

		String part_tmp_path = storage.full_path + "tmp_" + part_name + "/";
		String part_res_path = storage.full_path + part_name + "/";

		Poco::File(part_tmp_path).createDirectories();
		
		LOG_TRACE(storage.log, "Calculating primary expression.");

		/// Если для сортировки надо вычислить некоторые столбцы - делаем это.
		storage.primary_expr->execute(block);
		
		LOG_TRACE(storage.log, "Sorting by primary key.");

		/// Сортируем.
		sortBlock(block, storage.sort_descr);

		/// Наконец-то можно писать данные на диск.
		LOG_TRACE(storage.log, "Writing index.");

		/// Сначала пишем индекс. Индекс содержит значение PK для каждой index_granularity строки.
		{
			WriteBufferFromFile index(part_tmp_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, flags);

			typedef std::vector<const ColumnWithNameAndType *> PrimaryColumns;
			PrimaryColumns primary_columns;

			for (size_t i = 0, size = storage.sort_descr.size(); i < size; ++i)
				primary_columns.push_back(
					!storage.sort_descr[i].column_name.empty()
						? &block.getByName(storage.sort_descr[i].column_name)
						: &block.getByPosition(storage.sort_descr[i].column_number));

			for (size_t i = 0; i < rows; i += storage.index_granularity)
				for (PrimaryColumns::const_iterator it = primary_columns.begin(); it != primary_columns.end(); ++it)
					(*it)->type->serializeBinary((*(*it)->column)[i], index);
		}
		
		LOG_TRACE(storage.log, "Writing data.");
		
		for (size_t i = 0; i < columns; ++i)
		{
			const ColumnWithNameAndType & column = block.getByPosition(i);
			writeData(part_tmp_path, column.name, *column.type, *column.column);
		}
		
		LOG_TRACE(storage.log, "Renaming.");

		/// Переименовываем кусок.
		Poco::File(part_tmp_path).renameTo(part_res_path);

		/// Добавляем новый кусок в набор.
		{
			Poco::ScopedLock<Poco::FastMutex> lock(storage.data_parts_mutex);
			Poco::ScopedLock<Poco::FastMutex> lock_all(storage.all_data_parts_mutex);

			StorageMergeTree::DataPartPtr new_data_part = new StorageMergeTree::DataPart(storage);
			new_data_part->left_date = Yandex::DayNum_t(min_date);
			new_data_part->right_date = Yandex::DayNum_t(max_date);
			new_data_part->left = part_id;
			new_data_part->right = part_id;
			new_data_part->level = 0;
			new_data_part->name = part_name;
			new_data_part->size = (rows + storage.index_granularity - 1) / storage.index_granularity;
			new_data_part->modification_time = time(0);
			new_data_part->left_month = date_lut.toFirstDayNumOfMonth(new_data_part->left_date);
			new_data_part->right_month = date_lut.toFirstDayNumOfMonth(new_data_part->right_date);

			storage.data_parts.insert(new_data_part);
			storage.all_data_parts.insert(new_data_part);
		}
		
		/// Если на каждую запись делать по две итерации слияния, то дерево будет максимально компактно.
		storage.merge(2);
	}

	/// Записать данные одного столбца.
	void writeData(const String & path, const String & name, const IDataType & type, const IColumn & column, size_t level = 0)
	{
		String escaped_column_name = escapeForFileName(name);
		size_t size = column.size();
		
		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

			WriteBufferFromFile plain(path + size_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
			WriteBufferFromFile marks(path + size_name + ".mrk", 4096, flags);
			CompressedWriteBuffer compressed(plain);

			size_t prev_mark = 0;
			while (prev_mark < size)
			{
				/// Каждая засечка - это: (смещение в файле до начала сжатого блока, смещение внутри блока)
				writeIntBinary(plain.count(), marks);
				writeIntBinary(compressed.offset(), marks);
				
				type_arr->serializeOffsets(column, compressed, prev_mark, storage.index_granularity);
				prev_mark += storage.index_granularity;
			}
		}

		{
			WriteBufferFromFile plain(path + escaped_column_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
			WriteBufferFromFile marks(path + escaped_column_name + ".mrk", 4096, flags);
			CompressedWriteBuffer compressed(plain);

			size_t prev_mark = 0;
			while (prev_mark < size)
			{
				writeIntBinary(plain.count(), marks);
				writeIntBinary(compressed.offset(), marks);
				
				type.serializeBinary(column, compressed, prev_mark, storage.index_granularity);
				prev_mark += storage.index_granularity;
			}
		}
	}
};


/** Для записи куска, полученного слиянием нескольких других.
  * Данные уже отсортированы, относятся к одному месяцу, и пишутся в один кускок.
  */
class MergedBlockOutputStream : public IBlockOutputStream
{
public:
	MergedBlockOutputStream(StorageMergeTree & storage_,
		UInt16 min_date, UInt16 max_date, UInt64 min_part_id, UInt64 max_part_id, UInt32 level)
		: storage(storage_), marks_count(0), index_offset(0)
	{
		part_name = storage.getPartName(
			Yandex::DayNum_t(min_date), Yandex::DayNum_t(max_date),
			min_part_id, max_part_id, level);

		part_tmp_path = storage.full_path + "tmp_" + part_name + "/";
		part_res_path = storage.full_path + part_name + "/";

		Poco::File(part_tmp_path).createDirectories();

		index_stream = new WriteBufferFromFile(part_tmp_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY);

		for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
			addStream(it->first, *it->second);
	}

	void write(const Block & block)
	{
		size_t rows = block.rows();

		/// Сначала пишем индекс. Индекс содержит значение PK для каждой index_granularity строки.
		typedef std::vector<const ColumnWithNameAndType *> PrimaryColumns;
		PrimaryColumns primary_columns;

		for (size_t i = 0, size = storage.sort_descr.size(); i < size; ++i)
			primary_columns.push_back(
				!storage.sort_descr[i].column_name.empty()
					? &block.getByName(storage.sort_descr[i].column_name)
					: &block.getByPosition(storage.sort_descr[i].column_number));

		for (size_t i = index_offset; i < rows; i += storage.index_granularity)
		{
			for (PrimaryColumns::const_iterator it = primary_columns.begin(); it != primary_columns.end(); ++it)
			{
				(*it)->type->serializeBinary((*(*it)->column)[i], *index_stream);
			}
			
			++marks_count;
		}

		/// Теперь пишем данные.
		for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
		{
			const ColumnWithNameAndType & column = block.getByName(it->first);
			writeData(column.name, *column.type, *column.column);
		}

		index_offset = rows % storage.index_granularity
			? (storage.index_granularity - rows % storage.index_granularity)
			: 0;
	}

	void writeSuffix()
	{
		/// Заканчиваем запись.
		index_stream = NULL;
		column_streams.clear();
		
		if (marks_count == 0)
			throw Exception("Empty part", ErrorCodes::LOGICAL_ERROR);
		
		/// Переименовываем кусок.
		Poco::File(part_tmp_path).renameTo(part_res_path);

		/// А добавление нового куска в набор (и удаление исходных кусков) сделает вызывающая сторона.
	}

	BlockOutputStreamPtr clone() { throw Exception("Cannot clone MergedBlockOutputStream", ErrorCodes::NOT_IMPLEMENTED); }

	/// Сколько засечек уже записано.
	size_t marksCount()
	{
		return marks_count;
	}
	
private:
	StorageMergeTree & storage;
	String part_name;
	String part_tmp_path;
	String part_res_path;
	size_t marks_count;

	struct ColumnStream
	{
		ColumnStream(const String & data_path, const std::string & marks_path) :
			plain(data_path, DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY),
			compressed(plain),
			marks(marks_path, 4096, O_TRUNC | O_CREAT | O_WRONLY) {}

		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;
		WriteBufferFromFile marks;
	};

	typedef std::map<String, SharedPtr<ColumnStream> > ColumnStreams;
	ColumnStreams column_streams;

	SharedPtr<WriteBuffer> index_stream;

	/// Смещение до первой строчки блока, для которой надо записать индекс.
	size_t index_offset;


	void addStream(const String & name, const IDataType & type, size_t level = 0)
	{
		String escaped_column_name = escapeForFileName(name);
		
		/// Для массивов используются отдельные потоки для размеров.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
			String escaped_size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

			column_streams[size_name] = new ColumnStream(
				part_tmp_path + escaped_size_name + ".bin",
				part_tmp_path + escaped_size_name + ".mrk");

			addStream(name, *type_arr->getNestedType(), level + 1);
		}
		else
			column_streams[name] = new ColumnStream(
				part_tmp_path + escaped_column_name + ".bin",
				part_tmp_path + escaped_column_name + ".mrk");
	}


	/// Записать данные одного столбца.
	void writeData(const String & name, const IDataType & type, const IColumn & column, size_t level = 0)
	{
		size_t size = column.size();
		
		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

			ColumnStream & stream = *column_streams[size_name];

			size_t prev_mark = 0;
			while (prev_mark < size)
			{
				size_t limit = 0;
				
				/// Если есть index_offset, то первая засечка идёт не сразу, а после этого количества строк.
				if (prev_mark == 0 && index_offset != 0)
				{
					limit = index_offset;
				}
				else
				{
					limit = storage.index_granularity;
					writeIntBinary(stream.plain.count(), stream.marks);
					writeIntBinary(stream.compressed.offset(), stream.marks);
				}
				
				type_arr->serializeOffsets(column, stream.compressed, prev_mark, limit);
				prev_mark += limit;
			}
		}

		{
			ColumnStream & stream = *column_streams[name];

			size_t prev_mark = 0;
			while (prev_mark < size)
			{
				size_t limit = 0;

				/// Если есть index_offset, то первая засечка идёт не сразу, а после этого количества строк.
				if (prev_mark == 0 && index_offset != 0)
				{
					limit = index_offset;
				}
				else
				{
					limit = storage.index_granularity;
					writeIntBinary(stream.plain.count(), stream.marks);
					writeIntBinary(stream.compressed.offset(), stream.marks);
				}

				type.serializeBinary(column, stream.compressed, prev_mark, limit);
				prev_mark += limit;
			}
		}
	}
};

typedef Poco::SharedPtr<MergedBlockOutputStream> MergedBlockOutputStreamPtr;


/// Для чтения из одного куска. Для чтения сразу из многих, Storage использует сразу много таких объектов.
class MergeTreeBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// Параметры storage_ и owned_storage разделены, чтобы можно было сделать поток, не владеющий своим storage
	/// (например, поток, сливаящий куски). В таком случае сам storage должен следить, чтобы не удалить данные, пока их читают.
	MergeTreeBlockInputStream(const String & path_,	/// Путь к куску
		size_t block_size_, const Names & column_names_,
		StorageMergeTree & storage_, const StorageMergeTree::DataPartPtr & owned_data_part_,
		const MarkRanges & mark_ranges_, StoragePtr owned_storage)
		: IProfilingBlockInputStream(owned_storage), path(path_), block_size(block_size_), column_names(column_names_),
		storage(storage_), owned_data_part(owned_data_part_),
		mark_ranges(mark_ranges_), current_range(-1), rows_left_in_current_range(0)
	{
		LOG_TRACE(storage.log, "Reading " << mark_ranges.size() << " ranges from part " << owned_data_part->name
			<< ", up to " << (mark_ranges.back().end - mark_ranges.front().begin) * storage.index_granularity
			<< " rows starting from " << mark_ranges.front().begin * storage.index_granularity);
	}

	String getName() const { return "MergeTreeBlockInputStream"; }
	
	BlockInputStreamPtr clone()
	{
		return new MergeTreeBlockInputStream(path, block_size, column_names, storage, owned_data_part, mark_ranges, owned_storage);
	}
	
	/// Получает набор диапазонов засечек, вне которых не могут находиться ключи из заданного диапазона.
	static MarkRanges markRangesFromPkRange(const String & path,
									  size_t marks_count,
									  StorageMergeTree & storage,
								      PKCondition & key_condition)
	{
		MarkRanges res;
		
		/// Если индекс не используется.
		if (key_condition.alwaysTrue())
		{
			res.push_back(MarkRange(0, marks_count));
		}
		else
		{
			/// Читаем индекс.
			typedef AutoArray<Row> Index;
			size_t key_size = storage.sort_descr.size();
			Index index(marks_count);
			for (size_t i = 0; i < marks_count; ++i)
				index[i].resize(key_size);
			
			{
				String index_path = path + "primary.idx";
				ReadBufferFromFile index_file(index_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));
				
				for (size_t i = 0; i < marks_count; ++i)
				{
					for (size_t j = 0; j < key_size; ++j)
						storage.primary_key_sample.getByPosition(j).type->deserializeBinary(index[i][j], index_file);
				}
				
				if (!index_file.eof())
					throw Exception("index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);
			}

			/// В стеке всегда будут находиться непересекающиеся подозрительные отрезки, самый левый наверху (back).
			/// На каждом шаге берем левый отрезок и проверяем, подходит ли он.
			/// Если подходит, разбиваем его на более мелкие и кладем их в стек. Если нет - выбрасываем его.
			/// Если отрезок уже длиной в одну засечку, добавляем его в ответ и выбрасываем.
			std::vector<MarkRange> ranges_stack;
			ranges_stack.push_back(MarkRange(0, marks_count));
			while (!ranges_stack.empty())
			{
				MarkRange range = ranges_stack.back();
				ranges_stack.pop_back();
				
				bool may_be_true;
				if (range.end == marks_count)
					may_be_true = key_condition.mayBeTrueAfter(index[range.begin]);
				else
					may_be_true = key_condition.mayBeTrueInRange(index[range.begin], index[range.end]);
				
				if (!may_be_true)
					continue;
				
				if (range.end == range.begin + 1)
				{
					/// Увидели полезный промежуток между соседними засечками. Либо добавим его к последнему диапазону, либо начнем новый диапазон.
					if (res.empty() || range.begin - res.back().end > storage.min_marks_for_seek)
					{
						res.push_back(range);
					}
					else
					{
						res.back().end = range.end;
					}
				}
				else
				{
					/// Разбиваем отрезок и кладем результат в стек справа налево.
					size_t step = (range.end - range.begin - 1) / storage.settings.coarse_index_granularity + 1;
					size_t end;
					for (end = range.end; end > range.begin + step; end -= step)
					{
						ranges_stack.push_back(MarkRange(end - step, end));
					}
					ranges_stack.push_back(MarkRange(range.begin, end));
				}
			}
		}
		
		return res;
	}

protected:
	Block readImpl()
	{
		Block res;

		/// Если нужно, переходим к следующему диапазону.
		if (rows_left_in_current_range == 0)
		{
			++current_range;
			if (static_cast<size_t>(current_range) == mark_ranges.size())
				return res;
			
			MarkRange & range = mark_ranges[current_range];
			rows_left_in_current_range = (range.end - range.begin) * storage.index_granularity;
			
			for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
				addStream(*it, *storage.getDataTypeByName(*it), range.begin);
		}

		/// Сколько строк читать для следующего блока.
		size_t max_rows_to_read = std::min(block_size, rows_left_in_current_range);

		/** Для некоторых столбцов файлы с данными могут отсутствовать.
		  * Это бывает для старых кусков, после добавления новых столбцов в структуру таблицы.
		  */
		bool has_missing_columns = false;
		bool has_normal_columns = false;

		for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
		{
			if (streams.end() == streams.find(*it))
			{
				has_missing_columns = true;
				continue;
			}

			has_normal_columns = true;

			ColumnWithNameAndType column;
			column.name = *it;
			column.type = storage.getDataTypeByName(*it);
			column.column = column.type->createColumn();
			readData(*it, *column.type, *column.column, max_rows_to_read);

			if (column.column->size())
				res.insert(column);
		}

		if (has_missing_columns && !has_normal_columns)
			throw Exception("All requested columns are missing", ErrorCodes::ALL_REQUESTED_COLUMNS_ARE_MISSING);

		if (res)
		{
			rows_left_in_current_range -= res.rows();

			/// Заполним столбцы, для которых нет файлов, значениями по-умолчанию.
			if (has_missing_columns)
			{
				size_t pos = 0;	/// Позиция, куда надо вставить недостающий столбец.
				for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it, ++pos)
				{
					if (streams.end() == streams.find(*it))
					{
						ColumnWithNameAndType column;
						column.name = *it;
						column.type = storage.getDataTypeByName(*it);

						/** Нужно превратить константный столбец в полноценный, так как в части блоков (из других кусков),
						  *  он может быть полноценным (а то интерпретатор может посчитать, что он константный везде).
						  */
						column.column = dynamic_cast<IColumnConst &>(*column.type->createConstColumn(
							res.rows(), column.type->getDefault())).convertToFullColumn();

						res.insert(pos, column);
					}
				}
			}
		}

		if (!res || rows_left_in_current_range == 0)
		{
			rows_left_in_current_range = 0;
			/** Закрываем файлы (ещё до уничтожения объекта).
			  * Чтобы при создании многих источников, но одновременном чтении только из нескольких,
			  *  буферы не висели в памяти.
			  */
			streams.clear();
		}

		return res;
	}

private:
	const String path;
	size_t block_size;
	Names column_names;
	StorageMergeTree & storage;
	const StorageMergeTree::DataPartPtr owned_data_part;	/// Кусок не будет удалён, пока им владеет этот объект.
	MarkRanges mark_ranges; /// В каких диапазонах засечек читать.
	
	int current_range; /// Какой из mark_ranges сейчас читаем.
	size_t rows_left_in_current_range; /// Сколько строк уже прочитали из текущего элемента mark_ranges.

	struct Stream
	{
		Stream(const String & path_prefix, size_t mark_number)
			: plain(path_prefix + ".bin", std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path_prefix + ".bin").getSize())),
			compressed(plain)
		{
			if (mark_number)
			{
				/// Прочитаем из файла с засечками смещение в файле с данными.
				ReadBufferFromFile marks(path_prefix + ".mrk", MERGE_TREE_MARK_SIZE);
				marks.seek(mark_number * MERGE_TREE_MARK_SIZE);

				size_t offset_in_compressed_file = 0;
				size_t offset_in_decompressed_block = 0;

				readIntBinary(offset_in_compressed_file, marks);
				readIntBinary(offset_in_decompressed_block, marks);
				
				plain.seek(offset_in_compressed_file);
				compressed.next();
				compressed.position() += offset_in_decompressed_block;
			}
		}

		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;
	};

	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;


	void addStream(const String & name, const IDataType & type, size_t mark_number, size_t level = 0)
	{
		String escaped_column_name = escapeForFileName(name);

		/** Если файла с данными нет - то не будем пытаться открыть его.
		  * Это нужно, чтобы можно было добавлять новые столбцы к структуре таблицы без создания файлов для старых кусков.
		  */
		if (!Poco::File(path + escaped_column_name + ".bin").exists())
			return;

		/// Для массивов используются отдельные потоки для размеров.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
			String escaped_size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

			streams.insert(std::make_pair(size_name, new Stream(
				path + escaped_size_name,
				mark_number)));

			addStream(name, *type_arr->getNestedType(), mark_number, level + 1);
		}
		else
			streams.insert(std::make_pair(name, new Stream(
				path + escaped_column_name,
				mark_number)));
	}

	void readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read, size_t level = 0)
	{
		/// Для массивов требуется сначала десериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			type_arr->deserializeOffsets(
				column,
				streams[name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level)]->compressed,
				max_rows_to_read);

			if (column.size())
				readData(
					name,
					*type_arr->getNestedType(),
					dynamic_cast<ColumnArray &>(column).getData(),
					dynamic_cast<const ColumnArray &>(column).getOffsets()[column.size() - 1],
					level + 1);
		}
		else
			type.deserializeBinary(column, streams[name]->compressed, max_rows_to_read);
	}
};


StorageMergeTree::StorageMergeTree(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	const String & sign_column_,
	const StorageMergeTreeSettings & settings_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), columns(columns_),
	context(context_), primary_expr_ast(primary_expr_ast_->clone()),
	date_column_name(date_column_name_), sampling_expression(sampling_expression_), 
	index_granularity(index_granularity_),
	sign_column(sign_column_),
	settings(settings_),
	increment(full_path + "increment.txt"), log(&Logger::get("StorageMergeTree: " + name))
{
	min_marks_for_seek = (settings.min_rows_for_seek + index_granularity - 1) / index_granularity;
	min_marks_for_concurrent_read = (settings.min_rows_for_concurrent_read + index_granularity - 1) / index_granularity;

	/// создаём директорию, если её нет
	Poco::File(full_path).createDirectories();

	/// инициализируем описание сортировки
	sort_descr.reserve(primary_expr_ast->children.size());
	for (ASTs::iterator it = primary_expr_ast->children.begin();
		it != primary_expr_ast->children.end();
		++it)
	{
		String name = (*it)->getColumnName();
		sort_descr.push_back(SortColumnDescription(name, 1));
	}

	context.setColumns(*columns);
	primary_expr = new Expression(primary_expr_ast, context);
	primary_key_sample = primary_expr->getSampleBlock();

	merge_threads = new boost::threadpool::pool(settings.merging_threads);
	
	loadDataParts();
}

StoragePtr StorageMergeTree::create(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	const String & sign_column_,
	const StorageMergeTreeSettings & settings_)
{
	return (new StorageMergeTree(path_, name_, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_, index_granularity_, sign_column_, settings_))->thisPtr();
}


StorageMergeTree::~StorageMergeTree()
{
	joinMergeThreads();
}


BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query)
{
	return new MergeTreeBlockOutputStream(thisPtr());
}


BlockInputStreams StorageMergeTree::read(
	const Names & column_names_to_return,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	check(column_names_to_return);
	processed_stage = QueryProcessingStage::FetchColumns;
	
	PKCondition key_condition(query, context, sort_descr);
	PKCondition date_condition(query, context, SortDescription(1, SortColumnDescription(date_column_name, 1)));

	typedef std::vector<DataPartPtr> PartsList;
	PartsList parts;
	
	/// Выберем куски, в которых могут быть данные, удовлетворяющие date_condition.
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		
		for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
			if (date_condition.mayBeTrueInRange(Row(1, static_cast<UInt64>((*it)->left_date)),Row(1, static_cast<UInt64>((*it)->right_date))))
				parts.push_back(*it);
	}
	
	/// Семплирование.
	Names column_names_to_read = column_names_to_return;
	UInt64 sampling_column_value_limit = 0;
	typedef Poco::SharedPtr<ASTFunction> ASTFunctionPtr;
	ASTFunctionPtr filter_function;
	ExpressionPtr filter_expression;

	ASTSelectQuery & select = *dynamic_cast<ASTSelectQuery*>(&*query);
	if (select.sample_size)
	{
		double size = apply_visitor(FieldVisitorConvertToNumber<double>(),
										   dynamic_cast<ASTLiteral&>(*select.sample_size).value);
		if (size < 0)
			throw Exception("Negative sample size", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		if (size > 1)
		{
			size_t requested_count = apply_visitor(FieldVisitorConvertToNumber<UInt64>(), dynamic_cast<ASTLiteral&>(*select.sample_size).value);

			/// Узнаем, сколько строк мы бы прочли без семплирования.
			LOG_DEBUG(log, "Preliminary index scan with condition: " << key_condition.toString());
			size_t total_count = 0;
			for (size_t i = 0; i < parts.size(); ++i)
			{
				DataPartPtr & part = parts[i];
				MarkRanges ranges = MergeTreeBlockInputStream::markRangesFromPkRange(full_path + part->name + '/',
																					part->size,
																					*this,
																					key_condition);
				for (size_t j = 0; j < ranges.size(); ++j)
					total_count += ranges[j].end - ranges[j].begin;
			}
			total_count *= index_granularity;
			
			size = std::min(1., static_cast<double>(requested_count) / total_count);
			
			LOG_DEBUG(log, "Selected relative sample size: " << size);
		}
		
		UInt64 sampling_column_max = 0;
		DataTypePtr type = Expression(sampling_expression, context).getReturnTypes()[0];
		
		if (type->getName() == "UInt64")
			sampling_column_max = std::numeric_limits<UInt64>::max();
		else if (type->getName() == "UInt32")
			sampling_column_max = std::numeric_limits<UInt32>::max();
		else if (type->getName() == "UInt16")
			sampling_column_max = std::numeric_limits<UInt16>::max();
		else if (type->getName() == "UInt8")
			sampling_column_max = std::numeric_limits<UInt8>::max();
		else
			throw Exception("Invalid sampling column type in storage parameters: " + type->getName() + ". Must be unsigned integer type.", ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

		/// Добавим условие, чтобы отсечь еще что-нибудь при повторном просмотре индекса.
		sampling_column_value_limit = static_cast<UInt64>(size * sampling_column_max);
		if (!key_condition.addCondition(sampling_expression->getColumnName(),
			Range::RightBounded(sampling_column_value_limit, true)))
			throw Exception("Sampling column not in primary key", ErrorCodes::ILLEGAL_COLUMN);

		/// Выражение для фильтрации: sampling_expression <= sampling_column_value_limit

		ASTPtr filter_function_args = new ASTExpressionList;
		filter_function_args->children.push_back(sampling_expression);
		filter_function_args->children.push_back(new ASTLiteral(StringRange(), sampling_column_value_limit));

		filter_function = new ASTFunction;
		filter_function->name = "lessOrEquals";
		filter_function->arguments = filter_function_args;
		filter_function->children.push_back(filter_function->arguments);

		filter_expression = new Expression(filter_function, context);
		
		std::vector<String> add_columns = filter_expression->getRequiredColumns();
		column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
		std::sort(column_names_to_read.begin(), column_names_to_read.end());
		column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());
	}

	LOG_DEBUG(log, "Key condition: " << key_condition.toString());
	LOG_DEBUG(log, "Date condition: " << date_condition.toString());
	
	RangesInDataParts parts_with_ranges;
	
	/// Найдем, какой диапазон читать из каждого куска.
	size_t sum_marks = 0;
	size_t sum_ranges = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		DataPartPtr & part = parts[i];
		RangesInDataPart ranges(part);
		ranges.ranges = MergeTreeBlockInputStream::markRangesFromPkRange(full_path + part->name + '/',
		                                                                 part->size,
												                          *this,
		                                                                 key_condition);
		if (!ranges.ranges.empty())
		{
			parts_with_ranges.push_back(ranges);
			
			sum_ranges += ranges.ranges.size();
			for (size_t j = 0; j < ranges.ranges.size(); ++j)
			{
				sum_marks += ranges.ranges[j].end - ranges.ranges[j].begin;
			}
		}
	}
	
	LOG_DEBUG(log, "Selected " << parts.size() << " parts by date, " << parts_with_ranges.size() << " parts by key, "
			  << sum_marks << " marks to read from " << sum_ranges << " ranges");
	
	BlockInputStreams res;
	
	if (select.final)
	{
		res = spreadMarkRangesAmongThreadsCollapsing(parts_with_ranges, threads, column_names_to_read, max_block_size);
	}
	else
	{
		res = spreadMarkRangesAmongThreads(parts_with_ranges, threads, column_names_to_read, max_block_size);
	}
	
	if (select.sample_size)
	{
		for (size_t i = 0; i < res.size(); ++i)
		{
			BlockInputStreamPtr original_stream = res[i];
			BlockInputStreamPtr expression_stream = new ExpressionBlockInputStream(original_stream, filter_expression);
			BlockInputStreamPtr filter_stream = new FilterBlockInputStream(expression_stream, filter_function->getColumnName());
			res[i] = filter_stream;
		}
	}
	
	return res;
}


/// Примерно поровну распределить засечки между потоками.
BlockInputStreams StorageMergeTree::spreadMarkRangesAmongThreads(RangesInDataParts parts, size_t threads, const Names & column_names, size_t max_block_size)
{
	/// На всякий случай перемешаем куски.
	std::random_shuffle(parts.begin(), parts.end());
	
	/// Посчитаем засечки для каждого куска.
	std::vector<size_t> sum_marks_in_parts(parts.size());
	size_t sum_marks = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		/// Пусть отрезки будут перечислены справа налево, чтобы можно было выбрасывать самый левый отрезок с помощью pop_back().
		std::reverse(parts[i].ranges.begin(), parts[i].ranges.end());
		
		sum_marks_in_parts[i] = 0;
		for (size_t j = 0; j < parts[i].ranges.size(); ++j)
		{
			MarkRange & range = parts[i].ranges[j];
			sum_marks_in_parts[i] += range.end - range.begin;
		}
		sum_marks += sum_marks_in_parts[i];
	}
	
	BlockInputStreams res;
	
	if (sum_marks > 0)
	{
		size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;
		
		for (size_t i = 0; i < threads && !parts.empty(); ++i)
		{
			size_t need_marks = min_marks_per_thread;
			BlockInputStreams streams;
			
			/// Цикл по кускам.
			while (need_marks > 0 && !parts.empty())
			{
				RangesInDataPart & part = parts.back();
				size_t & marks_in_part = sum_marks_in_parts.back();
				
				/// Не будем брать из куска слишком мало строк.
				if (marks_in_part >= min_marks_for_concurrent_read &&
					need_marks < min_marks_for_concurrent_read)
					need_marks = min_marks_for_concurrent_read;
				
				/// Не будем оставлять в куске слишком мало строк.
				if (marks_in_part > need_marks &&
					marks_in_part - need_marks < min_marks_for_concurrent_read)
					need_marks = marks_in_part;
				
				/// Возьмем весь кусок, если он достаточно мал.
				if (marks_in_part <= need_marks)
				{
					/// Восстановим порядок отрезков.
					std::reverse(part.ranges.begin(), part.ranges.end());
					
					streams.push_back(new MergeTreeBlockInputStream(full_path + part.data_part->name + '/',
																	  max_block_size, column_names, *this,
																	  part.data_part, part.ranges, thisPtr()));
					need_marks -= marks_in_part;
					parts.pop_back();
					sum_marks_in_parts.pop_back();
					continue;
				}
				
				MarkRanges ranges_to_get_from_part;
				
				/// Цикл по отрезкам куска.
				while (need_marks > 0)
				{
					if (part.ranges.empty())
						throw Exception("Unexpected end of ranges while spreading marks among threads", ErrorCodes::LOGICAL_ERROR);
					
					MarkRange & range = part.ranges.back();
					size_t marks_in_range = range.end - range.begin;
				
					size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);
					ranges_to_get_from_part.push_back(MarkRange(range.begin, range.begin + marks_to_get_from_range));
					range.begin += marks_to_get_from_range;
					marks_in_part -= marks_to_get_from_range;
					need_marks -= marks_to_get_from_range;
					if (range.begin == range.end)
						part.ranges.pop_back();
				}
				
				streams.push_back(new MergeTreeBlockInputStream(full_path + part.data_part->name + '/',
																  max_block_size, column_names, *this,
																  part.data_part, ranges_to_get_from_part, thisPtr()));
			}
			
			if (streams.size() == 1)
				res.push_back(streams[0]);
			else
				res.push_back(new ConcatBlockInputStream(streams));
		}
		
		if (!parts.empty())
			throw Exception("Couldn't spread marks among threads", ErrorCodes::LOGICAL_ERROR);
	}
	
	return res;
}


/// Распределить засечки между потоками и сделать, чтобы в ответе все данные были сколлапсированы.
BlockInputStreams StorageMergeTree::spreadMarkRangesAmongThreadsCollapsing(RangesInDataParts parts, size_t threads, const Names & column_names, size_t max_block_size)
{
	BlockInputStreams streams;
	
	for (size_t part_index = 0; part_index < parts.size(); ++part_index)
	{
		RangesInDataPart & part = parts[part_index];
		
		streams.push_back(new ExpressionBlockInputStream(new MergeTreeBlockInputStream(full_path + part.data_part->name + '/',
													 max_block_size, column_names, *this,
													 part.data_part, part.ranges, thisPtr()), primary_expr));
	}
	
	return BlockInputStreams(1, new CollapsingSortedBlockInputStream(streams, sort_descr, sign_column, max_block_size));
}


String StorageMergeTree::getPartName(Yandex::DayNum_t left_date, Yandex::DayNum_t right_date, UInt64 left_id, UInt64 right_id, UInt64 level)
{
	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
	
	/// Имя директории для куска иммет вид: YYYYMMDD_YYYYMMDD_N_N_L.
	String res;
	{
		unsigned left_date_id = Yandex::Date2OrderedIdentifier(date_lut.fromDayNum(left_date));
		unsigned right_date_id = Yandex::Date2OrderedIdentifier(date_lut.fromDayNum(right_date));

		WriteBufferFromString wb(res);

		writeIntText(left_date_id, wb);
		writeChar('_', wb);
		writeIntText(right_date_id, wb);
		writeChar('_', wb);
		writeIntText(left_id, wb);
		writeChar('_', wb);
		writeIntText(right_id, wb);
		writeChar('_', wb);
		writeIntText(level, wb);
	}

	return res;
}


void StorageMergeTree::loadDataParts()
{
	LOG_DEBUG(log, "Loading data parts");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
		
	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
	data_parts.clear();

	static Poco::RegularExpression file_name_regexp("^(\\d{8})_(\\d{8})_(\\d+)_(\\d+)_(\\d+)");
	Poco::DirectoryIterator end;
	Poco::RegularExpression::MatchVec matches;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		std::string file_name = it.name();

		if (!(file_name_regexp.match(file_name, 0, matches) && 6 == matches.size()))
			continue;
			
		DataPartPtr part = new DataPart(*this);
		part->left_date = date_lut.toDayNum(Yandex::OrderedIdentifier2Date(file_name.substr(matches[1].offset, matches[1].length)));
		part->right_date = date_lut.toDayNum(Yandex::OrderedIdentifier2Date(file_name.substr(matches[2].offset, matches[2].length)));
		part->left = Poco::NumberParser::parseUnsigned64(file_name.substr(matches[3].offset, matches[3].length));
		part->right = Poco::NumberParser::parseUnsigned64(file_name.substr(matches[4].offset, matches[4].length));
		part->level = Poco::NumberParser::parseUnsigned(file_name.substr(matches[5].offset, matches[5].length));
		part->name = file_name;

		/// Размер - в количестве засечек.
		part->size = Poco::File(full_path + file_name + "/" + escapeForFileName(columns->front().first) + ".mrk").getSize()
			/ MERGE_TREE_MARK_SIZE;
			
		part->modification_time = it->getLastModified().epochTime();

		part->left_month = date_lut.toFirstDayNumOfMonth(part->left_date);
		part->right_month = date_lut.toFirstDayNumOfMonth(part->right_date);

		data_parts.insert(part);
	}

	all_data_parts = data_parts;

	/** Удаляем из набора актуальных кусков куски, которые содержатся в другом куске (которые были склеены),
	  *  но по каким-то причинам остались лежать в файловой системе.
	  * Удаление файлов будет произведено потом в методе clearOldParts.
	  */

	if (data_parts.size() >= 2)
	{
		DataParts::iterator prev_jt = data_parts.begin();
		DataParts::iterator curr_jt = prev_jt;
		++curr_jt;
		while (curr_jt != data_parts.end())
		{
			/// Куски данных за разные месяцы рассматривать не будем
			if ((*curr_jt)->left_month != (*curr_jt)->right_month
				|| (*curr_jt)->right_month != (*prev_jt)->left_month
				|| (*prev_jt)->left_month != (*prev_jt)->right_month)
			{
				++prev_jt;
				++curr_jt;
				continue;
			}

			if ((*curr_jt)->contains(**prev_jt))
			{
				LOG_WARNING(log, "Part " << (*curr_jt)->name << " contains " << (*prev_jt)->name);
				data_parts.erase(prev_jt);
				prev_jt = curr_jt;
				++curr_jt;
			}
			else if ((*prev_jt)->contains(**curr_jt))
			{
				LOG_WARNING(log, "Part " << (*prev_jt)->name << " contains " << (*curr_jt)->name);
				data_parts.erase(curr_jt++);
			}
			else
			{
				++prev_jt;
				++curr_jt;
			}
		}
	}

	LOG_DEBUG(log, "Loaded data parts (" << data_parts.size() << " items)");
}


void StorageMergeTree::clearOldParts()
{
	Poco::ScopedTry<Poco::FastMutex> lock;

	/// Если метод уже вызван из другого потока (или если all_data_parts прямо сейчас меняют), то можно ничего не делать.
	if (!lock.lock(&all_data_parts_mutex))
	{
		LOG_TRACE(log, "Already clearing or modifying old parts");
		return;
	}
	
	LOG_TRACE(log, "Clearing old parts");
	for (DataParts::iterator it = all_data_parts.begin(); it != all_data_parts.end();)
	{
		int ref_count = it->referenceCount();
		LOG_TRACE(log, (*it)->name << ": ref_count = " << ref_count);
		if (ref_count == 1)		/// После этого ref_count не может увеличиться.
		{
			LOG_DEBUG(log, "Removing part " << (*it)->name);
			
			(*it)->remove();
			all_data_parts.erase(it++);
		}
		else
			++it;
	}
}


void StorageMergeTree::merge(size_t iterations, bool async)
{
	bool while_can = false;
	if (iterations == 0){
		while_can = true;
		iterations = settings.merging_threads;
	}
	
	for (size_t i = 0; i < iterations; ++i)
		merge_threads->schedule(boost::bind(&StorageMergeTree::mergeThread, this, while_can));
	
	if (!async)
		joinMergeThreads();
}


void StorageMergeTree::mergeThread(bool while_can)
{
	try
	{
		std::vector<DataPartPtr> parts;
		while (selectPartsToMerge(parts, false) ||
			   selectPartsToMerge(parts, true))
		{
			mergeParts(parts);

			/// Удаляем старые куски.
			parts.clear();
			clearOldParts();
			
			if (!while_can)
				break;
		}
	}
	catch (const Exception & e)
	{
		LOG_ERROR(log, "Code: " << e.code() << ". " << e.displayText() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString());
	}
	catch (const Poco::Exception & e)
	{
		LOG_ERROR(log, "Poco::Exception: " << e.code() << ". " << e.displayText());
	}
	catch (const std::exception & e)
	{
		LOG_ERROR(log, "std::exception: " << e.what());
	}
	catch (...)
	{
		LOG_ERROR(log, "Unknown exception");
	}
}


void StorageMergeTree::joinMergeThreads()
{
	LOG_DEBUG(log, "Waiting for merge thread to finish.");
	merge_threads->wait();
}


/// Выбираем отрезок из не более чем max_parts_to_merge_at_once кусков так, чтобы максимальный размер был меньше чем в max_size_ratio_to_merge_parts раз больше суммы остальных.
/// Это обеспечивает в худшем случае время O(n log n) на все слияния, независимо от выбора сливаемых кусков, порядка слияния и добавления.
/// При max_parts_to_merge_at_once >= log(max_rows_to_merge_parts/index_granularity)/log(max_size_ratio_to_merge_parts),
/// несложно доказать, что всегда будет что сливать, пока количество кусков больше
/// log(max_rows_to_merge_parts/index_granularity)/log(max_size_ratio_to_merge_parts)*(количество кусков размером больше max_rows_to_merge_parts).
/// Дальше эвристики.
/// Будем выбирать максимальный по включению подходящий отрезок.
/// Из всех таких выбираем отрезок с минимальным максимумом размера.
/// Из всех таких выбираем отрезок с минимальным минимумом размера.
/// Из всех таких выбираем отрезок с максимальной длиной.
bool StorageMergeTree::selectPartsToMerge(std::vector<DataPartPtr> & parts, bool merge_anything_for_old_months)
{
	LOG_DEBUG(log, "Selecting parts to merge");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
	
	size_t min_max = -1U;
	size_t min_min = -1U;
	int max_len = 0;
	DataParts::iterator best_begin;
	bool found = false;
	
	Yandex::DayNum_t now_day = date_lut.toDayNum(time(0));
	Yandex::DayNum_t now_month = date_lut.toFirstDayNumOfMonth(now_day);
		
	/// Сколько кусков, начиная с текущего, можно включить в валидный отрезок, начинающийся левее текущего куска.
	/// Нужно для определения максимальности по включению.
	int max_count_from_left = 0;
	
	/// Левый конец отрезка.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		const DataPartPtr & first_part = *it;
		
		max_count_from_left = std::max(0, max_count_from_left - 1);
		
		/// Кусок не занят и достаточно мал.
		if (first_part->currently_merging ||
			first_part->size * index_granularity > settings.max_rows_to_merge_parts)
			continue;
		
		/// Кусок в одном месяце.
		if (first_part->left_month != first_part->right_month)
		{
			LOG_WARNING(log, "Part " << first_part->name << " spans more than one month");
			continue;
		}
		
		/// Самый длинный валидный отрезок, начинающийся здесь.
		size_t cur_longest_max = -1U;
		size_t cur_longest_min = -1U;
		int cur_longest_len = 0;
		
		/// Текущий отрезок, не обязательно валидный.
		size_t cur_max = first_part->size;
		size_t cur_min = first_part->size;
		size_t cur_sum = first_part->size;
		int cur_len = 1;
		
		Yandex::DayNum_t month = first_part->left_month;
		UInt64 cur_id = first_part->right;
		
		/// Этот месяц кончился хотя бы день назад.
		bool is_old_month = now_day - now_month >= 1 && now_month > month;
		
		/// Правый конец отрезка.
		DataParts::iterator jt = it;
		for (++jt; jt != data_parts.end() && cur_len < static_cast<int>(settings.max_parts_to_merge_at_once); ++jt)
		{
			const DataPartPtr & last_part = *jt;
			
			/// Кусок не занят, достаточно мал и в одном правильном месяце.
			if (last_part->currently_merging ||
				last_part->size * index_granularity > settings.max_rows_to_merge_parts ||
				last_part->left_month != last_part->right_month ||
				last_part->left_month != month)
				break;
			
			/// Кусок правее предыдущего.
			if (last_part->left < cur_id)
			{
				LOG_WARNING(log, "Part " << last_part->name << " intersects previous part");
				break;
			}
			
			cur_max = std::max(cur_max, last_part->size);
			cur_min = std::min(cur_min, last_part->size);
			cur_sum += last_part->size;
			++cur_len;
			cur_id = last_part->right;
			
			/// Если отрезок валидный, то он самый длинный валидный, начинающийся тут.
			if (cur_len >= 2 &&
				(static_cast<double>(cur_max) / (cur_sum - cur_max) < settings.max_size_ratio_to_merge_parts ||
				(is_old_month && merge_anything_for_old_months))) /// За старый месяц объединяем что угодно, если разрешено.
			{
				cur_longest_max = cur_max;
				cur_longest_min = cur_min;
				cur_longest_len = cur_len;
			}
		}
		
		/// Это максимальный по включению валидный отрезок.
		if (cur_longest_len > max_count_from_left)
		{
			max_count_from_left = cur_longest_len;
			
			if (!found ||
				std::make_pair(std::make_pair(cur_longest_max, cur_longest_min), -cur_longest_len) <
				std::make_pair(std::make_pair(min_max, min_min), -max_len))
			{
				found = true;
				min_max = cur_longest_max;
				min_min = cur_longest_min;
				max_len = cur_longest_len;
				best_begin = it;
			}
		}
	}
	
	if (found)
	{
		parts.clear();
		
		DataParts::iterator it = best_begin;
		for (int i = 0; i < max_len; ++i)
		{
			parts.push_back(*it);
			parts.back()->currently_merging = true;
			++it;
		}
		
		LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->name << " to " << parts.back()->name);
	}
	else
	{
		LOG_DEBUG(log, "No parts to merge");
	}
	
	return found;
}


void StorageMergeTree::mergeParts(std::vector<DataPartPtr> parts)
{
	LOG_DEBUG(log, "Merging " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name);

	Names all_column_names;
	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
		all_column_names.push_back(it->first);

	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();

	StorageMergeTree::DataPartPtr new_data_part = new DataPart(*this);
	new_data_part->left_date = std::numeric_limits<UInt16>::max();
	new_data_part->right_date = std::numeric_limits<UInt16>::min();
	new_data_part->left = parts.front()->left;
	new_data_part->right = parts.back()->right;
	new_data_part->level = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		new_data_part->level = std::max(new_data_part->level, parts[i]->level);
		new_data_part->left_date = std::min(new_data_part->left_date, parts[i]->left_date);
		new_data_part->right_date = std::max(new_data_part->right_date, parts[i]->right_date);
	}
	++new_data_part->level;
	new_data_part->name = getPartName(
		new_data_part->left_date, new_data_part->right_date, new_data_part->left, new_data_part->right, new_data_part->level);
	new_data_part->left_month = date_lut.toFirstDayNumOfMonth(new_data_part->left_date);
	new_data_part->right_month = date_lut.toFirstDayNumOfMonth(new_data_part->right_date);

	/** Читаем из всех кусков, сливаем и пишем в новый.
	  * Попутно вычисляем выражение для сортировки.
	  */
	BlockInputStreams src_streams;

	for (size_t i = 0; i < parts.size(); ++i)
	{
		MarkRanges ranges(1, MarkRange(0, parts[i]->size));
		src_streams.push_back(new ExpressionBlockInputStream(new MergeTreeBlockInputStream(
			full_path + parts[i]->name + '/', DEFAULT_BLOCK_SIZE, all_column_names, *this, parts[i], ranges, StoragePtr()), primary_expr));
	}

	BlockInputStreamPtr merged_stream = sign_column.empty()
		? new MergingSortedBlockInputStream(src_streams, sort_descr, DEFAULT_BLOCK_SIZE)
		: new CollapsingSortedBlockInputStream(src_streams, sort_descr, sign_column, DEFAULT_BLOCK_SIZE);
	
	MergedBlockOutputStreamPtr to = new MergedBlockOutputStream(*this,
		new_data_part->left_date, new_data_part->right_date, new_data_part->left, new_data_part->right, new_data_part->level);

	copyData(*merged_stream, *to);

	new_data_part->size = to->marksCount();
	new_data_part->modification_time = time(0);

	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

		/// Добавляем новый кусок в набор.
		
		for (size_t i = 0; i < parts.size(); ++i)
		{
			if (data_parts.end() == data_parts.find(parts[i]))
				throw Exception("Logical error: cannot find data part " + parts[i]->name + " in list", ErrorCodes::LOGICAL_ERROR);
		}

		data_parts.insert(new_data_part);
		all_data_parts.insert(new_data_part);
		
		for (size_t i = 0; i < parts.size(); ++i)
		{
			data_parts.erase(data_parts.find(parts[i]));
		}
	}

	LOG_TRACE(log, "Merged " << parts.size() << " parts: from " << parts.front()->name << " to " << parts.back()->name);
}


void StorageMergeTree::rename(const String & new_path_to_db, const String & new_name)
{
	joinMergeThreads();
	
	std::string new_full_path = new_path_to_db + escapeForFileName(new_name) + '/';
	
	Poco::File(full_path).renameTo(new_full_path);
	
	path = new_path_to_db;
	full_path = new_full_path;
	name = new_name;
	
	increment.setPath(full_path + "increment.txt");
}


void StorageMergeTree::dropImpl()
{
	joinMergeThreads();
	
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	data_parts.clear();
	all_data_parts.clear();

	Poco::File(full_path).remove(true);
}

}
