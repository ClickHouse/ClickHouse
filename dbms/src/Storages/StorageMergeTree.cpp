#include <boost/bind.hpp>
#include <numeric>

#include <Poco/DirectoryIterator.h>
#include <Poco/NumberParser.h>
#include <Poco/Ext/scopedTry.h>

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

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/sortBlock.h>

#include <DB/Storages/StorageMergeTree.h>


#define MERGE_TREE_MARK_SIZE (2 * sizeof(size_t))


namespace DB
{

class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	MergeTreeBlockOutputStream(StorageMergeTree & storage_) : storage(storage_), flags(O_TRUNC | O_CREAT | O_WRONLY)
	{
	}

	void write(const Block & block)
	{
		storage.check(block);

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

	BlockOutputStreamPtr clone() { return new MergeTreeBlockOutputStream(storage); }

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
		
		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

			WriteBufferFromFile plain(path + size_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
			WriteBufferFromFile marks(path + size_name + ".mrk", 4096, flags);
			CompressedWriteBuffer compressed(plain);

			size_t prev_mark = 0;
			type_arr->serializeOffsets(column, compressed,
				boost::bind(&MergeTreeBlockOutputStream::writeCallback, this,
					boost::ref(prev_mark), boost::ref(plain), boost::ref(compressed), boost::ref(marks)));
		}

		{
			WriteBufferFromFile plain(path + escaped_column_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
			WriteBufferFromFile marks(path + escaped_column_name + ".mrk", 4096, flags);
			CompressedWriteBuffer compressed(plain);

			size_t prev_mark = 0;
			type.serializeBinary(column, compressed,
				boost::bind(&MergeTreeBlockOutputStream::writeCallback, this,
					boost::ref(prev_mark), boost::ref(plain), boost::ref(compressed), boost::ref(marks)));
		}
	}

	/// Вызывается каждые index_granularity строк и пишет в файл с засечками (.mrk).
	size_t writeCallback(size_t & prev_mark,
		WriteBufferFromFile & plain,
		CompressedWriteBuffer & compressed,
		WriteBufferFromFile & marks)
	{
		/// Каждая засечка - это: (смещение в файле до начала сжатого блока, смещение внутри блока)

		writeIntBinary(plain.count(), marks);
		writeIntBinary(compressed.offset(), marks);

		prev_mark += storage.index_granularity;
		return prev_mark;
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
		: storage(storage_), index_offset(0)
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
			for (PrimaryColumns::const_iterator it = primary_columns.begin(); it != primary_columns.end(); ++it)
				(*it)->type->serializeBinary((*(*it)->column)[i], *index_stream);

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
		
		/// Переименовываем кусок.
		Poco::File(part_tmp_path).renameTo(part_res_path);

		/// А добавление нового куска в набор (и удаление исходных кусков) сделает вызывающая сторона.
	}

	BlockOutputStreamPtr clone() { throw Exception("Cannot clone MergedBlockOutputStream", ErrorCodes::NOT_IMPLEMENTED); }

private:
	StorageMergeTree & storage;
	String part_name;
	String part_tmp_path;
	String part_res_path;

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
		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

			ColumnStream & stream = *column_streams[size_name];

			size_t prev_mark = 0;
			type_arr->serializeOffsets(column, stream.compressed,
				boost::bind(&MergedBlockOutputStream::writeCallback, this,
					boost::ref(prev_mark), boost::ref(stream.plain), boost::ref(stream.compressed), boost::ref(stream.marks)));
		}

		{
			ColumnStream & stream = *column_streams[name];

			size_t prev_mark = 0;
			type.serializeBinary(column, stream.compressed,
				boost::bind(&MergedBlockOutputStream::writeCallback, this,
					boost::ref(prev_mark), boost::ref(stream.plain), boost::ref(stream.compressed), boost::ref(stream.marks)));
		}
	}
	

	/// Вызывается каждые index_granularity строк и пишет в файл с засечками (.mrk).
	size_t writeCallback(size_t & prev_mark,
		WriteBufferFromFile & plain,
		CompressedWriteBuffer & compressed,
		WriteBufferFromFile & marks)
	{
		/// Если есть index_offset, то первая засечка идёт не сразу, а после этого количества строк.
		if (prev_mark == 0 && index_offset != 0)
		{
			prev_mark = index_offset;
			return prev_mark;
		}
		
		/// Каждая засечка - это: (смещение в файле до начала сжатого блока, смещение внутри блока)

		writeIntBinary(plain.count(), marks);
		writeIntBinary(compressed.offset(), marks);

		prev_mark += storage.index_granularity;
		return prev_mark;
	}
};


/** Диапазон с открытыми или закрытыми концами; возможно, неограниченный.
  * Определяет, какую часть данных читать, при наличии индекса.
  */
struct Range
{
	Field left;				/// левая граница, если есть
	Field right;			/// правая граница, если есть
	bool left_bounded;		/// ограничен ли слева
	bool right_bounded; 	/// ограничен ли справа
	bool left_included; 	/// включает левую границу, если есть
	bool right_included;	/// включает правую границу, если есть

	/// Всё множество.
	Range() : left(), right(), left_bounded(false), right_bounded(false), left_included(false), right_included(false) {}

	/// Одна точка.
	Range(const Field & point) : left(point), right(point), left_bounded(true), right_bounded(true), left_included(true), right_included(true) {}

	/// Установить левую границу.
	void setLeft(const Field & point, bool included)
	{
		left = point;
		left_bounded = true;
		left_included = included;
	}

	/// Установить правую границу.
	void setRight(const Field & point, bool included)
	{
		right = point;
		right_bounded = true;
		right_included = included;
	}

	/// x входит в range
	bool contains(const Field & x)
	{
		return !leftThan(x) && !rightThan(x);
	}

	/// x находится левее
	bool rightThan(const Field & x)
	{
		return (left_bounded
			? !(boost::apply_visitor(FieldVisitorGreater(), x, left) || (left_included && x == left))
			: false);
	}

	/// x находится правее
	bool leftThan(const Field & x)
	{
		return (right_bounded
			? !(boost::apply_visitor(FieldVisitorLess(), x, right) || (right_included && x == right))
			: false);
	}

	/// Пересекает отрезок
	bool intersectsSegment(const Field & segment_left, const Field & segment_right)
	{
		if (!left_bounded)
			return contains(segment_left);
		if (!right_bounded)
			return contains(segment_right);

		return (boost::apply_visitor(FieldVisitorLess(), segment_left, right) || (right_included && segment_left == right))
			&& (boost::apply_visitor(FieldVisitorGreater(), segment_right, left) || (left_included && segment_right == left));
	}

	String toString()
	{
		std::stringstream str;

		if (!left_bounded)
			str << "(-inf, ";
		else
			str << (left_included ? '[' : '(') << boost::apply_visitor(FieldVisitorToString(), left) << ", ";

		if (!right_bounded)
			str << "+inf)";
		else
			str << boost::apply_visitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

		return str.str();
	}
};


/// Для чтения из одного куска. Для чтения сразу из многих, Storage использует сразу много таких объектов.
class MergeTreeBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergeTreeBlockInputStream(const String & path_,	/// Путь к куску
		size_t block_size_, const Names & column_names_,
		StorageMergeTree & storage_, const StorageMergeTree::DataPartPtr & owned_data_part_,
		size_t mark_number_, size_t rows_limit_)
		: path(path_), block_size(block_size_), column_names(column_names_),
		storage(storage_), owned_data_part(owned_data_part_),
		mark_number(mark_number_), rows_limit(rows_limit_), rows_read(0)
	{
		if (mark_number == 0 && rows_limit == std::numeric_limits<size_t>::max())
		{
			LOG_TRACE(storage.log, "Reading from part " << owned_data_part->name << ", all rows.");
		}
		else
		{
			LOG_TRACE(storage.log, "Reading from part " << owned_data_part->name
				<< ", up to " << rows_limit << " rows from row " << mark_number * storage.index_granularity << ".");
		}
	}

	String getName() const { return "MergeTreeBlockInputStream"; }
	
	BlockInputStreamPtr clone()
	{
		return new MergeTreeBlockInputStream(path, block_size, column_names, storage, owned_data_part, mark_number, rows_limit);
	}
	
	/// Получает диапазон засечек, вне которого не могут находиться ключи из заданного диапазона.
	static void markRangeFromPkRange(const String & path,
									  size_t marks_count,
									  StorageMergeTree & storage,
								      Row & requested_pk_prefix,
								      Range & requested_pk_range,
									  size_t & out_first_mark,
									  size_t & out_last_mark)
	{
		size_t last_mark_in_file = (marks_count == 0) ? 0 : (marks_count - 1);
		
		/// Если индекс не используется.
		if (requested_pk_prefix.size() == 0 && !requested_pk_range.left_bounded && !requested_pk_range.right_bounded)
		{
			out_first_mark = 0;
			out_last_mark = last_mark_in_file;
		}
		else
		{
			/// Читаем PK, и на основе primary_prefix, primary_range определим mark_number и rows_limit.
			
			ssize_t min_mark_number = -1;
			ssize_t max_mark_number = -1;
			
			String index_path = path + "primary.idx";
			ReadBufferFromFile index(index_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));
			
			size_t prefix_size = requested_pk_prefix.size();
			Row pk(storage.sort_descr.size());
			Row pk_prefix(prefix_size);
			
			for (size_t current_mark_number = 0; !index.eof(); ++current_mark_number)
			{
				/// Читаем очередное значение PK
				Row pk(storage.sort_descr.size());
				for (size_t i = 0, size = pk.size(); i < size; ++i)
					storage.primary_key_sample.getByPosition(i).type->deserializeBinary(pk[i], index);
				
				pk_prefix.assign(pk.begin(), pk.begin() + pk_prefix.size());
				
				if (pk_prefix < requested_pk_prefix)
				{
					min_mark_number = current_mark_number;
				}
				else if (pk_prefix == requested_pk_prefix)
				{
					if (requested_pk_range.rightThan(pk[prefix_size]))
					{
						min_mark_number = current_mark_number;
					}
					else if (requested_pk_range.leftThan(pk[prefix_size]))
					{
						max_mark_number = current_mark_number == 0 ? 0 : (current_mark_number - 1);
						break;
					}
				}
				else
				{
					max_mark_number = current_mark_number == 0 ? 0 : (current_mark_number - 1);
					break;
				}
			}
			
			out_first_mark = min_mark_number == -1 ? 0 : min_mark_number;
			out_last_mark = max_mark_number == -1 ? last_mark_in_file : max_mark_number;
		}
	}

protected:
	Block readImpl()
	{
		Block res;

		if (rows_read == rows_limit)
			return res;

		/// Если файлы не открыты, то открываем их.
		if (streams.empty())
			for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
				addStream(*it, *storage.getDataTypeByName(*it));

		/// Сколько строк читать для следующего блока.
		size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);

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
			rows_read += res.rows();

			/// Заполним столбцы, для которых нет файлов, значениями по-умолчанию.
			if (has_missing_columns)
			{
				for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
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

						res.insert(column);
					}
				}
			}
		}

		if (!res || rows_read == rows_limit)
		{
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
	size_t mark_number;		/// С какой засечки читать данные
	size_t rows_limit;		/// Максимальное количество строк, которых можно прочитать

	size_t rows_read;

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


	void addStream(const String & name, const IDataType & type, size_t level = 0)
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

			addStream(name, *type_arr->getNestedType(), level + 1);
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
	ASTPtr & primary_expr_ast_, const String & date_column_name_,
	size_t index_granularity_,
	const String & sign_column_,
	const StorageMergeTreeSettings & settings_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), columns(columns_),
	context(context_), primary_expr_ast(primary_expr_ast_->clone()),
	date_column_name(date_column_name_), index_granularity(index_granularity_),
	sign_column(sign_column_),
	settings(settings_),
	increment(full_path + "increment.txt"), log(&Logger::get("StorageMergeTree: " + name))
{
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


StorageMergeTree::~StorageMergeTree()
{
	joinMergeThreads();
}


BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query)
{
	return new MergeTreeBlockOutputStream(*this);
}


/// Собирает список отношений в конъюнкции в секции WHERE для определения того, можно ли использовать индекс.
static void getRelationsFromConjunction(ASTPtr & node, ASTs & relations)
{
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*node))
	{
		if (func->name == "equals"
			|| func->name == "less" || func->name == "greater"
			|| func->name == "lessOrEquals" || func->name == "greaterOrEquals")
		{
			relations.push_back(node);
		}
		else if (func->name == "and")
		{
			/// Обходим рекурсивно.
			ASTs & args = dynamic_cast<ASTExpressionList &>(*func->arguments).children;

			getRelationsFromConjunction(args.at(0), relations);
			getRelationsFromConjunction(args.at(1), relations);
		}
	}
}


/** Получить значение константного выражения.
  * Вернуть false, если выражение не константно.
  */
static bool getConstant(ASTPtr & expr, Block & block_with_constants, Field & value)
{
	String column_name = expr->getColumnName();

	if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*expr))
	{
		/// литерал
		value = lit->value;
		return true;
	}
	else if (block_with_constants.has(column_name) && block_with_constants.getByName(column_name).column->isConst())
	{
		/// выражение, вычислившееся в константу
		value = (*block_with_constants.getByName(column_name).column)[0];
		return true;
	}
	else
		return false;
}


/** Получить значение константного аргумента функции вида f(name, const_expr) или f(const_expr, name).
  * block_with_constants содержит вычисленные значения константных выражений.
  * Вернуть false, если такого нет.
  */
static bool getConstantArgument(ASTs & args, Block & block_with_constants, Field & rhs)
{
	if (args.size() != 2)
		return false;
	
	return getConstant(args[0], block_with_constants, rhs)
		|| getConstant(args[1], block_with_constants, rhs);
}


/// Составить диапазон возможных значений для столбца на основе секции WHERE с вычисленными константными выражениями.
static Range getRangeForColumn(ASTs & relations, const String & column_name, Block & block_with_constants)
{
	Range range;
	
	for (ASTs::iterator jt = relations.begin(); jt != relations.end(); ++jt)
	{
		ASTFunction * func = dynamic_cast<ASTFunction *>(&**jt);
		if (!func)
			continue;

		ASTs & args = dynamic_cast<ASTExpressionList &>(*func->arguments).children;

		if (args.size() != 2)
			continue;

		/// Шаблон: col rel const или const rel col
		bool inverted;
		if (column_name == args[0]->getColumnName())
			inverted = false;
		else if (column_name == args[1]->getColumnName())
			inverted = true;
		else
			continue;

		Field rhs;
		if (!getConstantArgument(args, block_with_constants, rhs))
			continue;
		
		if (func->name == "equals")
		{
			range = Range(rhs);
			break;
		}
		else if (func->name == "greater")
			!inverted ? range.setLeft(rhs, false) : range.setRight(rhs, false);
		else if (func->name == "greaterOrEquals")
			!inverted ? range.setLeft(rhs, true) : range.setRight(rhs, true);
		else if (func->name == "less")
			!inverted ? range.setRight(rhs, false) : range.setLeft(rhs, false);
		else if (func->name == "lessOrEquals")
			!inverted ? range.setRight(rhs, true) : range.setLeft(rhs, true);
	}

	return range;
}


/** Выделяет значение, которому должен быть равен столбец на основе секции WHERE с вычисленными константными выражениями.
  * Если такого нет - возвращает false.
  */
static bool getEqualityForColumn(ASTs & relations, const String & column_name, Block & block_with_constants, Field & value)
{
	for (ASTs::iterator jt = relations.begin(); jt != relations.end(); ++jt)
	{
		ASTFunction * func = dynamic_cast<ASTFunction *>(&**jt);
		if (!func || func->name != "equals")
			continue;

		ASTs & args = dynamic_cast<ASTExpressionList &>(*func->arguments).children;

		if (args.size() != 2)
			continue;

		if (args[0]->getColumnName() != column_name && args[1]->getColumnName() != column_name)
			continue;

		if (getConstantArgument(args, block_with_constants, value))
			return true;
		else
			continue;
	}
	
	return false;
}


void StorageMergeTree::getIndexRanges(ASTPtr & query, Range & date_range, Row & primary_prefix, Range & primary_range)
{
	/** Вычисление выражений, зависящих только от констант.
	  * Чтобы индекс мог использоваться, если написано, например WHERE Date = toDate(now()).
	  */
	Expression expr_for_constant_folding(query, context);
	Block block_with_constants;

	/// В блоке должен быть хотя бы один столбец, чтобы у него было известно число строк.
	ColumnWithNameAndType dummy_column;
	dummy_column.name = "_dummy";
	dummy_column.type = new DataTypeUInt8;
	dummy_column.column = new ColumnConstUInt8(1, 0);
	block_with_constants.insert(dummy_column);
	
	expr_for_constant_folding.execute(block_with_constants, 0, true);

	/// Выделяем из конъюнкции в секции WHERE все отношения.
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*query);
	if (select.where_expression)
	{
		ASTs relations;
		getRelationsFromConjunction(select.where_expression, relations);

		/// Ищем отношения, которые могут быть использованы для индекса по дате.
		date_range = getRangeForColumn(relations, date_column_name, block_with_constants);

		/** Теперь ищем отношения, которые могут быть использованы для первичного ключа.
		  * Сначала находим максимальное количество отношений равенства константе для первых столбцов PK.
		  */
		for (SortDescription::const_iterator it = sort_descr.begin(); it != sort_descr.end(); ++it)
		{
			Field rhs;
			if (getEqualityForColumn(relations, it->column_name, block_with_constants, rhs))
				primary_prefix.push_back(rhs);
			else
				break;
		}

		/// Если не для всех столбцов PK записано равенство, то ищем отношения для следующего столбца PK.
		if (primary_prefix.size() < sort_descr.size())
			primary_range = getRangeForColumn(relations, sort_descr[primary_prefix.size()].column_name, block_with_constants);
	}

	LOG_DEBUG(log, "Date range: " << date_range.toString());

	std::stringstream primary_prefix_str;
	for (Row::const_iterator it = primary_prefix.begin(); it != primary_prefix.end(); ++it)
		primary_prefix_str << (it != primary_prefix.begin() ? ", " : "") << boost::apply_visitor(FieldVisitorToString(), *it);

	LOG_DEBUG(log, "Primary key prefix: (" << primary_prefix_str.str() << ")");

	if (primary_prefix.size() < sort_descr.size())
	{
		LOG_DEBUG(log, "Primary key range for column " << sort_descr[primary_prefix.size()].column_name << ": " << primary_range.toString());
	}
}


BlockInputStreams StorageMergeTree::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	/// Диапазон дат.
	Range date_range;
	/// Префикс первичного ключа, для которого требуется равенство. Может быть пустым.
	Row primary_prefix;
	/// Диапазон следующего после префикса столбца первичного ключа.
	Range primary_range;

	getIndexRanges(query, date_range, primary_prefix, primary_range);

	typedef std::vector<DataPartRange> PartsRanges;
	PartsRanges parts;
	
	/// Выберем куски, в которых могут быть данные для date_range.
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		
		for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
			if (date_range.intersectsSegment(static_cast<UInt64>((*it)->left_date), static_cast<UInt64>((*it)->right_date)))
				parts.push_back(DataPartRange(*it, 0, 0));
	}
	
	/// Найдем, какой диапазон читать из каждого куска.
	size_t sum_marks = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		DataPartRange & part = parts[i];
		MergeTreeBlockInputStream::markRangeFromPkRange(full_path + part.data_part->name + '/',
		                                                part.data_part->size,
												         *this,
		                                                primary_prefix,
		                                                primary_range,
		                                                part.first_mark,
		                                                part.last_mark);
		sum_marks += part.last_mark - part.first_mark + 1;
	}
	
	LOG_DEBUG(log, "Selected " << parts.size() << " parts, " << sum_marks << " marks to read");
	
	/// В случайном порядке поровну поделим засечки между потоками.
	size_t effective_threads = std::min(static_cast<size_t>(threads), sum_marks);
	std::random_shuffle(parts.begin(), parts.end());
	
	BlockInputStreams res;
	size_t cur_part = 0;
	/// Сколько зесечек уже забрали из parts[cur_part].
	size_t cur_pos = 0;
	size_t marks_spread = 0;
	
	for (size_t i = 0; i < effective_threads && marks_spread < sum_marks; ++i)
	{
		size_t need_marks = sum_marks * (i + 1) / effective_threads - marks_spread;
		BlockInputStreams streams;
		
		while (need_marks > 0)
		{
			if (cur_part >= parts.size())
				throw Exception("Can't spread marks among threads", ErrorCodes::LOGICAL_ERROR);
			
			DataPartRange & part = parts[cur_part];
			size_t marks_left_in_part = part.last_mark - part.first_mark + 1 - cur_pos;
			
			if (marks_left_in_part == 0)
			{
				++cur_part;
				cur_pos = 0;
				continue;
			}
			
			size_t marks_to_get_from_part = std::min(marks_left_in_part, need_marks);
			
			/// Не будем оставлять в куске слишком мало строк.
			if ((marks_left_in_part - marks_to_get_from_part) * index_granularity < settings.min_rows_for_concurrent_read)
				marks_to_get_from_part = marks_left_in_part;
			
			streams.push_back(new MergeTreeBlockInputStream(full_path + part.data_part->name + '/',
			                                                max_block_size, column_names, *this,
			                                                part.data_part, part.first_mark + cur_pos,
			                                                marks_to_get_from_part * index_granularity));
			
			marks_spread += marks_to_get_from_part;
			if (marks_to_get_from_part > need_marks)
				need_marks = 0;
			else
				need_marks -= marks_to_get_from_part;
			cur_pos += marks_to_get_from_part;
		}
		
		if (streams.size() == 1)
			res.push_back(streams[0]);
		else
			res.push_back(new ConcatBlockInputStream(streams));
	}
	
	if (marks_spread != sum_marks || cur_part + 1 != parts.size() || cur_pos != parts.back().last_mark - parts.back().first_mark + 1)
		throw Exception("Can't spread marks among threads", ErrorCodes::LOGICAL_ERROR);
	
	return res;
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
		while (selectPartsToMerge(parts))
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
bool StorageMergeTree::selectPartsToMerge(std::vector<DataPartPtr> & parts)
{
	LOG_DEBUG(log, "Selecting parts to merge");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	size_t min_max = -1U;
	size_t min_min = -1U;
	int max_len = 0;
	DataParts::iterator best_begin;
	bool found = false;
		
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
				static_cast<double>(cur_max) / (cur_sum - cur_max) < settings.max_size_ratio_to_merge_parts)
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
	new_data_part->left_date = parts.front()->left_date;
	new_data_part->right_date = parts.back()->right_date;
	new_data_part->left = parts.front()->left;
	new_data_part->right = parts.back()->right;
	new_data_part->level = 0;
	new_data_part->size = 0;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		new_data_part->level = std::max(new_data_part->level, parts[i]->level);
		new_data_part->size += parts[i]->size;
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
		src_streams.push_back(new ExpressionBlockInputStream(new MergeTreeBlockInputStream(
			full_path + parts[i]->name + '/', DEFAULT_BLOCK_SIZE, all_column_names, *this, parts[i], 0, std::numeric_limits<size_t>::max()), primary_expr));
	}

	BlockInputStreamPtr merged_stream = sign_column.empty()
		? new MergingSortedBlockInputStream(src_streams, sort_descr, DEFAULT_BLOCK_SIZE)
		: new CollapsingSortedBlockInputStream(src_streams, sort_descr, sign_column, DEFAULT_BLOCK_SIZE);
	
	BlockOutputStreamPtr to = new MergedBlockOutputStream(*this,
		new_data_part->left_date, new_data_part->right_date, new_data_part->left, new_data_part->right, new_data_part->level);

	copyData(*merged_stream, *to);

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

	LOG_TRACE(log, "Merged " << parts.size() << " parts: from" << parts.front()->name << " to " << parts.back()->name);
}


void StorageMergeTree::drop()
{
	joinMergeThreads();
	
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	data_parts.clear();
	all_data_parts.clear();

	Poco::File(full_path).remove(true);
}

}
