#include <Poco/Path.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeNested.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnNested.h>

#include <DB/Storages/StorageLog.h>

#include <DB/DataTypes/DataTypeString.h>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION 	".bin"
#define DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION	".mrk"
#define DBMS_STORAGE_LOG_MARKS_FILE_NAME 		"__marks.mrk"


namespace DB
{

using Poco::SharedPtr;


class LogBlockInputStream : public IProfilingBlockInputStream
{
public:
	LogBlockInputStream(
		size_t block_size_, const Names & column_names_, StorageLog & storage_,
		size_t mark_number_, size_t rows_limit_, size_t max_read_buffer_size_)
		: block_size(block_size_), column_names(column_names_), storage(storage_),
		mark_number(mark_number_), rows_limit(rows_limit_), current_mark(mark_number_), max_read_buffer_size(max_read_buffer_size_) {}

	String getName() const { return "LogBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Log(" << storage.getTableName() << ", " << &storage << ", " << mark_number << ", " << rows_limit;

		for (const auto & name : column_names)
			res << ", " << name;

		res << ")";
		return res.str();
	}

protected:
	Block readImpl();
private:
	size_t block_size;
	Names column_names;
	StorageLog & storage;
	size_t mark_number;		/// С какой засечки читать данные
	size_t rows_limit;		/// Максимальное количество строк, которых можно прочитать
	size_t rows_read = 0;
	size_t current_mark;
	size_t max_read_buffer_size;

	struct Stream
	{
		Stream(const std::string & data_path, size_t offset, size_t max_read_buffer_size)
			: plain(data_path, std::min(max_read_buffer_size, Poco::File(data_path).getSize())),
			compressed(plain)
		{
			if (offset)
				plain.seek(offset);
		}

		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;
	};

	typedef std::map<std::string, std::unique_ptr<Stream> > FileStreams;
	FileStreams streams;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read, size_t level = 0, bool read_offsets = true);
};


class LogBlockOutputStream : public IBlockOutputStream
{
public:
	LogBlockOutputStream(StorageLog & storage_)
		: storage(storage_),
		lock(storage.rwlock), marks_stream(storage.marks_file.path(), 4096, O_APPEND | O_CREAT | O_WRONLY)
	{
		for (const auto & column : storage.getColumnsList())
			addStream(column.name, *column.type);
	}

	~LogBlockOutputStream()
	{
		try
		{
			writeSuffix();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}

	void write(const Block & block);
	void writeSuffix();

private:
	StorageLog & storage;
	Poco::ScopedWriteRWLock lock;
	bool done = false;

	struct Stream
	{
		Stream(const std::string & data_path, size_t max_compress_block_size) :
			plain(data_path, max_compress_block_size, O_APPEND | O_CREAT | O_WRONLY),
			compressed(plain)
		{
			plain_offset = Poco::File(data_path).getSize();
		}

		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;

		size_t plain_offset;	/// Сколько байт было в файле на момент создания LogBlockOutputStream.

		void finalize()
		{
			compressed.next();
			plain.next();
		}
	};

	typedef std::vector<std::pair<size_t, Mark> > MarksForColumns;

	typedef std::map<std::string, std::unique_ptr<Stream> > FileStreams;
	FileStreams streams;

	typedef std::set<std::string> OffsetColumns;

	WriteBufferFromFile marks_stream; /// Объявлен ниже lock, чтобы файл открывался при захваченном rwlock.

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void writeData(const String & name, const IDataType & type, const IColumn & column, MarksForColumns & out_marks, OffsetColumns & offset_columns, size_t level = 0);
	void writeMarks(MarksForColumns marks);
};


Block LogBlockInputStream::readImpl()
{
	Block res;

	if (rows_read == rows_limit)
		return res;

	/// Если файлы не открыты, то открываем их.
	if (streams.empty())
	{
		Poco::ScopedReadRWLock lock(storage.rwlock);

		for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
			if (*it != storage._table_column_name) /// Для виртуального столбца не надо ничего открывать
				addStream(*it, *storage.getDataTypeByName(*it));
	}

	bool has_virtual_column_table = false;
	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
		if (*it == storage._table_column_name)
			has_virtual_column_table = true;

	/// Сколько строк читать для следующего блока.
	size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);
	const Marks & marks = storage.getMarksWithRealRowCount();
	std::pair<String, size_t> current_table;

	/// Отдельно обрабатываем виртуальный столбец
	if (has_virtual_column_table)
	{
		size_t current_row = rows_read;
		if (mark_number > 0)
			current_row += marks[mark_number-1].rows;
		while (current_mark < marks.size() && marks[current_mark].rows <= current_row)
			++current_mark;

		current_table = storage.getTableFromMark(current_mark);
		current_table.second = std::min(current_table.second, marks.size() - 1);
		max_rows_to_read = std::min(max_rows_to_read, marks[current_table.second].rows - current_row);
	}

	/// Указатели на столбцы смещений, общие для столбцов из вложенных структур данных
	typedef std::map<std::string, ColumnPtr> OffsetColumns;
	OffsetColumns offset_columns;

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		/// Виртуальный столбец не надо считывать с жесткого диска
		if (*it == storage._table_column_name)
			continue;

		ColumnWithNameAndType column;
		column.name = *it;
		column.type = storage.getDataTypeByName(*it);

		bool read_offsets = true;

		/// Для вложенных структур запоминаем указатели на столбцы со смещениями
		if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&*column.type))
		{
			String name = DataTypeNested::extractNestedTableName(column.name);

			if (offset_columns.count(name) == 0)
				offset_columns[name] = new ColumnArray::ColumnOffsets_t;
			else
				read_offsets = false; /// на предыдущих итерациях смещения уже считали вызовом readData

			column.column = new ColumnArray(type_arr->getNestedType()->createColumn(), offset_columns[name]);
		}
		else
			column.column = column.type->createColumn();

		readData(*it, *column.type, *column.column, max_rows_to_read, 0, read_offsets);

		if (column.column->size())
			res.insert(column);
	}

	/// Отдельно обрабатываем виртуальный столбец
	if (has_virtual_column_table)
	{
		size_t rows = max_rows_to_read;
		if (res.columns() > 0)
			rows = res.rows();
		if (rows > 0)
		{
			ColumnPtr column_ptr = ColumnConst<String> (rows, current_table.first, new DataTypeString).convertToFullColumn();
			ColumnWithNameAndType column(column_ptr, new DataTypeString, storage._table_column_name);
			res.insert(column);
		}
	}

	if (res)
		rows_read += res.rows();

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


void LogBlockInputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		if (!streams.count(size_name))
			streams.emplace(size_name, std::unique_ptr<Stream>(new Stream(
				storage.files[size_name].data_file.path(),
				mark_number
					? storage.files[size_name].marks[mark_number].offset
					: 0,
				max_read_buffer_size)));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams[name].reset(new Stream(
			storage.files[name].data_file.path(),
			mark_number
				? storage.files[name].marks[mark_number].offset
				: 0,
			max_read_buffer_size));
}


void LogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read,
									size_t level, bool read_offsets)
{
	/// Для массивов требуется сначала десериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		if (read_offsets)
		{
			type_arr->deserializeOffsets(
				column,
				streams[DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level)]->compressed,
				max_rows_to_read);
		}

		if (column.size())
			readData(
				name,
				*type_arr->getNestedType(),
				typeid_cast<ColumnArray &>(column).getData(),
				typeid_cast<const ColumnArray &>(column).getOffsets()[column.size() - 1],
				level + 1);
	}
	else
		type.deserializeBinary(column, streams[name]->compressed, max_rows_to_read, 0);	/// TODO Использовать avg_value_size_hint.
}


void LogBlockOutputStream::write(const Block & block)
{
	storage.check(block, true);

	/// Множество записанных столбцов со смещениями, чтобы не писать общие для вложенных структур столбцы несколько раз
	OffsetColumns offset_columns;

	MarksForColumns marks;
	marks.reserve(storage.files.size());
	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);
		writeData(column.name, *column.type, *column.column, marks, offset_columns);
	}
	writeMarks(marks);
}


void LogBlockOutputStream::writeSuffix()
{
	if (done)
		return;
	done = true;

	/// Заканчиваем запись.
	marks_stream.next();

	for (FileStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		it->second->finalize();

	std::vector<Poco::File> column_files;
	for (auto & pair : streams)
		column_files.push_back(storage.files[pair.first].data_file);
	column_files.push_back(storage.marks_file);

	storage.file_checker.update(column_files.begin(), column_files.end());

	streams.clear();
}


void LogBlockOutputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		if (!streams.count(size_name))
			streams.emplace(size_name, std::unique_ptr<Stream>(new Stream(
				storage.files[size_name].data_file.path(), storage.max_compress_block_size)));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams[name].reset(new Stream(storage.files[name].data_file.path(), storage.max_compress_block_size));
}


void LogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column, MarksForColumns & out_marks,
										OffsetColumns & offset_columns, size_t level)
{
	/// Для массивов требуется сначала сериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

		if (offset_columns.count(size_name) == 0)
		{
			offset_columns.insert(size_name);

			Mark mark;
			mark.rows = (storage.files[size_name].marks.empty() ? 0 : storage.files[size_name].marks.back().rows) + column.size();
			mark.offset = streams[size_name]->plain_offset + streams[size_name]->plain.count();

			out_marks.push_back(std::make_pair(storage.files[size_name].column_index, mark));

			type_arr->serializeOffsets(column, streams[size_name]->compressed);
			streams[size_name]->compressed.next();
		}

		writeData(name, *type_arr->getNestedType(), typeid_cast<const ColumnArray &>(column).getData(), out_marks, offset_columns, level + 1);
	}
	else
	{
		Mark mark;
		mark.rows = (storage.files[name].marks.empty() ? 0 : storage.files[name].marks.back().rows) + column.size();
		mark.offset = streams[name]->plain_offset + streams[name]->plain.count();

		out_marks.push_back(std::make_pair(storage.files[name].column_index, mark));

		type.serializeBinary(column, streams[name]->compressed);
		streams[name]->compressed.next();
	}
}

static bool ColumnIndexLess(const std::pair<size_t, Mark> & a, const std::pair<size_t, Mark> & b)
{
	return a.first < b.first;
}

void LogBlockOutputStream::writeMarks(MarksForColumns marks)
{
	if (marks.size() != storage.files.size())
		throw Exception("Wrong number of marks generated from block. Makes no sense.", ErrorCodes::LOGICAL_ERROR);

	sort(marks.begin(), marks.end(), ColumnIndexLess);

	for (size_t i = 0; i < marks.size(); ++i)
	{
		if (marks[i].first != i)
			throw Exception("Invalid marks generated from block. Makes no sense.", ErrorCodes::LOGICAL_ERROR);

		Mark mark = marks[i].second;

		writeIntBinary(mark.rows, marks_stream);
		writeIntBinary(mark.offset, marks_stream);

		storage.files[storage.column_names[i]].marks.push_back(mark);
	}
}


StorageLog::StorageLog(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	size_t max_compress_block_size_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	path(path_), name(name_), columns(columns_),
	loaded_marks(false), max_compress_block_size(max_compress_block_size_),
	file_checker(path + escapeForFileName(name) + '/' + "sizes.json", *this)
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

	/// создаём файлы, если их нет
	Poco::File(path + escapeForFileName(name) + '/').createDirectories();

	for (const auto & column : getColumnsList())
		addFile(column.name, *column.type);

	marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_MARKS_FILE_NAME);
}

StoragePtr StorageLog::create(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	size_t max_compress_block_size_)
{
	return (new StorageLog{
		path_, name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_,
		max_compress_block_size_
	})->thisPtr();
}

StoragePtr StorageLog::create(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	size_t max_compress_block_size_)
{
	return (new StorageLog{
		path_, name_, columns_,
		{}, {}, ColumnDefaults{},
		max_compress_block_size_
	})->thisPtr();
}


void StorageLog::addFile(const String & column_name, const IDataType & type, size_t level)
{
	if (files.end() != files.find(column_name))
		throw Exception("Duplicate column with name " + column_name + " in constructor of StorageLog.",
			ErrorCodes::DUPLICATE_COLUMN);

	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_column_suffix = ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		String size_name = DataTypeNested::extractNestedTableName(column_name) + size_column_suffix;

		if (files.end() == files.find(size_name))
		{
			ColumnData & column_data = files.insert(std::make_pair(size_name, ColumnData())).first->second;
			column_data.column_index = column_names.size();
			column_data.data_file = Poco::File(
				path + escapeForFileName(name) + '/' + escapeForFileName(DataTypeNested::extractNestedTableName(column_name)) + size_column_suffix + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);

			column_names.push_back(size_name);
		}

		addFile(column_name, *type_arr->getNestedType(), level + 1);
	}
	else
	{
		ColumnData & column_data = files.insert(std::make_pair(column_name, ColumnData())).first->second;
		column_data.column_index = column_names.size();
		column_data.data_file = Poco::File(
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);

		column_names.push_back(column_name);
	}
}


void StorageLog::loadMarks()
{
	Poco::ScopedWriteRWLock lock(rwlock);

	if (loaded_marks)
		return;

	typedef std::vector<Files_t::iterator> FilesByIndex;
	FilesByIndex files_by_index(files.size());
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
	{
		files_by_index[it->second.column_index] = it;
	}

	if (marks_file.exists())
	{
		size_t file_size = marks_file.getSize();
		if (file_size % (files.size() * sizeof(Mark)) != 0)
			throw Exception("Size of marks file is inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);

		int marks_count = file_size / (files.size() * sizeof(Mark));

		for (size_t i = 0; i < files_by_index.size(); ++i)
		{
			files_by_index[i]->second.marks.reserve(marks_count);
		}

		ReadBufferFromFile marks_rb(marks_file.path(), 32768);
		while (!marks_rb.eof())
		{
			for (size_t i = 0; i < files_by_index.size(); ++i)
			{
				Mark mark;
				readIntBinary(mark.rows, marks_rb);
				readIntBinary(mark.offset, marks_rb);
				files_by_index[i]->second.marks.push_back(mark);
			}
		}
	}

	loaded_marks = true;
}


size_t StorageLog::marksCount()
{
	return files.begin()->second.marks.size();
}


void StorageLog::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
	Poco::ScopedWriteRWLock lock(rwlock);

	/// Переименовываем директорию с данными.
	Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_table_name));

	path = new_path_to_db;
	name = new_table_name;
	file_checker.setPath(path + escapeForFileName(name) + '/' + "sizes.json");

	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
	{
		it->second.data_file = Poco::File(path + escapeForFileName(name) + '/' + Poco::Path(it->second.data_file.path()).getFileName());
	}

	marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_MARKS_FILE_NAME);
}


const Marks & StorageLog::getMarksWithRealRowCount() const
{
	const String & column_name = columns->front().name;
	const IDataType & column_type = *columns->front().type;
	String file_name;

	/** Засечки достаём из первого столбца.
	  * Если это - массив, то берём засечки, соответствующие размерам, а не внутренностям массивов.
	  */

	if (typeid_cast<const DataTypeArray *>(&column_type))
	{
		file_name = DataTypeNested::extractNestedTableName(column_name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX "0";
	}
	else
	{
		file_name = column_name;
	}

	Files_t::const_iterator it = files.find(file_name);
	if (files.end() == it)
		throw Exception("Cannot find file " + file_name, ErrorCodes::LOGICAL_ERROR);

	return it->second.marks;
}


BlockInputStreams StorageLog::read(
	size_t from_mark,
	size_t to_mark,
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	/** Если читаем все данные в один поток, то засечки не требуются.
	  * Отсутствие необходимости загружать засечки позволяет уменьшить потребление памяти при использовании таблицы типа ChunkMerger.
	  */
	bool read_all_data_in_one_thread = (threads == 1 && from_mark == 0 && to_mark == std::numeric_limits<size_t>::max());
	if (!read_all_data_in_one_thread)
		loadMarks();

	bool has_virtual_column = false;
	Names real_column_names;
	for (const auto & column : column_names)
		if (column != _table_column_name)
			real_column_names.push_back(column);
		else
			has_virtual_column = true;

	/// Если есть виртуальный столбец и нет остальных, то ничего проверять не надо
	if (!(has_virtual_column && real_column_names.size() == 0))
		check(real_column_names);

	processed_stage = QueryProcessingStage::FetchColumns;

	Poco::ScopedReadRWLock lock(rwlock);

	BlockInputStreams res;

	if (read_all_data_in_one_thread)
	{
		res.push_back(new LogBlockInputStream(
			max_block_size,
			column_names,
			*this,
			0, std::numeric_limits<size_t>::max(),
			settings.max_read_buffer_size));
	}
	else
	{
		const Marks & marks = getMarksWithRealRowCount();
		size_t marks_size = marks.size();

		if (to_mark == std::numeric_limits<size_t>::max())
			to_mark = marks_size;

		if (to_mark > marks_size || to_mark < from_mark)
			throw Exception("Marks out of range in StorageLog::read", ErrorCodes::LOGICAL_ERROR);

		if (threads > to_mark - from_mark)
			threads = to_mark - from_mark;

		for (size_t thread = 0; thread < threads; ++thread)
		{
			res.push_back(new LogBlockInputStream(
				max_block_size,
				column_names,
				*this,
				from_mark + thread * (to_mark - from_mark) / threads,
				marks[from_mark + (thread + 1) * (to_mark - from_mark) / threads - 1].rows -
					((thread == 0 && from_mark == 0)
						? 0
						: marks[from_mark + thread * (to_mark - from_mark) / threads - 1].rows),
				settings.max_read_buffer_size));
		}
	}

	return res;
}


BlockInputStreams StorageLog::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	return read(0, std::numeric_limits<size_t>::max(), column_names,
				query, context, settings, processed_stage,
				max_block_size, threads);
}


BlockOutputStreamPtr StorageLog::write(
	ASTPtr query)
{
	loadMarks();
	return new LogBlockOutputStream(*this);
}

bool StorageLog::checkData() const
{
	Poco::ScopedReadRWLock lock(const_cast<Poco::RWLock &>(rwlock));

	return file_checker.check();
}

}
