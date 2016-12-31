#include <DB/Storages/StorageLog.h>

#include <DB/Common/Exception.h>
#include <DB/Common/StringUtils.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnNullable.h>

#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION 	".bin"
#define DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION	".mrk"
#define DBMS_STORAGE_LOG_MARKS_FILE_NAME 		"__marks.mrk"
#define DBMS_STORAGE_LOG_NULL_MARKS_FILE_NAME 	"__null_marks.mrk"
#define DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION ".null"


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
	extern const int DUPLICATE_COLUMN;
	extern const int SIZES_OF_MARKS_FILES_ARE_INCONSISTENT;
}


class LogBlockInputStream : public IProfilingBlockInputStream
{
public:
	LogBlockInputStream(
		size_t block_size_, const Names & column_names_, StorageLog & storage_,
		size_t mark_number_, size_t rows_limit_, size_t max_read_buffer_size_)
		: block_size(block_size_),
		column_names(column_names_),
		column_types(column_names.size()),
		storage(storage_),
		mark_number(mark_number_),
		null_mark_number(0),
		rows_limit(rows_limit_),
		max_read_buffer_size(max_read_buffer_size_)
	{
	}

	LogBlockInputStream(
		size_t block_size_, const Names & column_names_, StorageLog & storage_,
		size_t mark_number_, size_t null_mark_number_, size_t rows_limit_, size_t max_read_buffer_size_)
		: block_size(block_size_),
		column_names(column_names_),
		column_types(column_names.size()),
		storage(storage_),
		mark_number(mark_number_),
		null_mark_number(null_mark_number_),
		rows_limit(rows_limit_),
		max_read_buffer_size(max_read_buffer_size_)
	{
	}

	String getName() const override { return "Log"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Log(" << storage.getTableName() << ", " << &storage << ", " << mark_number << ", " << rows_limit;

		for (const auto & name : column_names)
			res << ", " << name;

		res << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

private:
	size_t block_size;
	Names column_names;
	DataTypes column_types;
	StorageLog & storage;
	size_t mark_number;		/// С какой засечки читать данные
	size_t null_mark_number;
	size_t rows_limit;		/// Максимальное количество строк, которых можно прочитать
	size_t rows_read = 0;
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

	using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;
	FileStreams streams;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read, size_t level = 0, bool read_offsets = true);
};


class LogBlockOutputStream : public IBlockOutputStream
{
public:
	LogBlockOutputStream(StorageLog & storage_)
		: storage(storage_),
		lock(storage.rwlock),
		marks_stream(storage.marks_file.path(), 4096, O_APPEND | O_CREAT | O_WRONLY),
		null_marks_stream(storage.has_nullable_columns ?
			std::make_unique<WriteBufferFromFile>(storage.null_marks_file.path(), 4096, O_APPEND | O_CREAT | O_WRONLY) : nullptr)
	{
		for (const auto & column : storage.getColumnsList())
			addStream(column.name, *column.type);
	}

	~LogBlockOutputStream() override
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

	void write(const Block & block) override;
	void writeSuffix() override;

private:
	StorageLog & storage;
	Poco::ScopedWriteRWLock lock;
	bool done = false;

	struct Stream
	{
		Stream(const std::string & data_path, size_t max_compress_block_size) :
			plain(data_path, max_compress_block_size, O_APPEND | O_CREAT | O_WRONLY),
			compressed(plain, CompressionMethod::LZ4, max_compress_block_size)
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

	using MarksForColumns = std::vector<std::pair<size_t, Mark> >;

	using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;
	FileStreams streams;

	using OffsetColumns = std::set<std::string>;

	WriteBufferFromFile marks_stream; /// Объявлен ниже lock, чтобы файл открывался при захваченном rwlock.
	std::unique_ptr<WriteBufferFromFile> null_marks_stream;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void addNullStream(const String & name);
	void writeData(const String & name, const IDataType & type, const IColumn & column,
		MarksForColumns & out_marks, MarksForColumns & out_null_marks,
		OffsetColumns & offset_columns, size_t level = 0);
	void writeMarks(MarksForColumns marks, bool write_null_marks);
};


Block LogBlockInputStream::readImpl()
{
	Block res;

	if (rows_read == rows_limit)
		return res;

	/// If there are no files in the folder, the table is empty.
	if (Poco::DirectoryIterator(storage.getFullPath()) == Poco::DirectoryIterator())
		return res;

	/// Если файлы не открыты, то открываем их.
	if (streams.empty())
	{
		Poco::ScopedReadRWLock lock(storage.rwlock);

		for (size_t i = 0, size = column_names.size(); i < size; ++i)
		{
			const auto & name = column_names[i];
			column_types[i] = storage.getDataTypeByName(name);
			addStream(name, *column_types[i]);
		}
	}

	/// Сколько строк читать для следующего блока.
	size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);

	/// Указатели на столбцы смещений, общие для столбцов из вложенных структур данных
	using OffsetColumns = std::map<std::string, ColumnPtr>;
	OffsetColumns offset_columns;

	for (size_t i = 0, size = column_names.size(); i < size; ++i)
	{
		const auto & name = column_names[i];

		ColumnWithTypeAndName column;
		column.name = name;
		column.type = column_types[i];

		bool read_offsets = true;

		const IDataType * observed_type;
		bool is_nullable;

		if (column.type->isNullable())
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
				offset_columns[name] = std::make_shared<ColumnArray::ColumnOffsets_t>();
			else
				read_offsets = false; /// на предыдущих итерациях смещения уже считали вызовом readData

			column.column = std::make_shared<ColumnArray>(type_arr->getNestedType()->createColumn(), offset_columns[name]);
			if (is_nullable)
				column.column = std::make_shared<ColumnNullable>(column.column, std::make_shared<ColumnUInt8>());
		}
		else
			column.column = column.type->createColumn();

		try
		{
			readData(name, *column.type, *column.column, max_rows_to_read, 0, read_offsets);
		}
		catch (Exception & e)
		{
			e.addMessage("while reading column " + name + " at " + storage.path + escapeForFileName(storage.name));
			throw;
		}

		if (column.column->size())
			res.insert(std::move(column));
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
	if (type.isNullable())
	{
		/// First create the stream that handles the null map of the given column.
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const IDataType & nested_type = *nullable_type.getNestedType();

		std::string filename = name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION;

		streams.emplace(filename, std::make_unique<Stream>(
			storage.files[filename].data_file.path(),
			null_mark_number
				? storage.files[filename].marks[null_mark_number].offset
				: 0,
			max_read_buffer_size));

		/// Then create the stream that handles the data of the given column.
		addStream(name, nested_type, level);
	}
	else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		/// Для массивов используются отдельные потоки для размеров.
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
		streams[name] = std::make_unique<Stream>(
			storage.files[name].data_file.path(),
			mark_number
				? storage.files[name].marks[mark_number].offset
				: 0,
			max_read_buffer_size);
}


void LogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read,
									size_t level, bool read_offsets)
{
	if (type.isNullable())
	{
		/// First read from the null map.
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const IDataType & nested_type = *nullable_type.getNestedType();

		ColumnNullable & nullable_col = static_cast<ColumnNullable &>(column);
		IColumn & nested_col = *nullable_col.getNestedColumn();

		DataTypeUInt8{}.deserializeBinary(*nullable_col.getNullMapColumn(),
			streams[name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION]->compressed, max_rows_to_read, 0);
		/// Then read data.
		readData(name, nested_type, nested_col, max_rows_to_read, level, read_offsets);
	}
	else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		/// Для массивов требуется сначала десериализовать размеры, а потом значения.
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
	marks.reserve(storage.file_count);

	MarksForColumns null_marks;
	if (null_marks_stream)
		null_marks.reserve(storage.null_file_count);

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithTypeAndName & column = block.getByPosition(i);
		writeData(column.name, *column.type, *column.column, marks, null_marks, offset_columns);
	}

	writeMarks(marks, false);
	if (null_marks_stream)
		writeMarks(null_marks, true);
}


void LogBlockOutputStream::writeSuffix()
{
	if (done)
		return;
	done = true;

	/// Заканчиваем запись.
	marks_stream.next();
	if (null_marks_stream)
		null_marks_stream->next();

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
	if (type.isNullable())
	{
		/// First create the stream that handles the null map of the given column.
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const IDataType & nested_type = *nullable_type.getNestedType();

		std::string filename = name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION;
		streams.emplace(filename, std::make_unique<Stream>(storage.files[filename].data_file.path(),
			storage.max_compress_block_size));

		/// Then create the stream that handles the data of the given column.
		addStream(name, nested_type, level);
	}
	else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		/// Для массивов используются отдельные потоки для размеров.
		String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		if (!streams.count(size_name))
			streams.emplace(size_name, std::unique_ptr<Stream>(new Stream(
				storage.files[size_name].data_file.path(), storage.max_compress_block_size)));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams[name] = std::make_unique<Stream>(storage.files[name].data_file.path(), storage.max_compress_block_size);
}


void LogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column,
	MarksForColumns & out_marks, MarksForColumns & out_null_marks,
	OffsetColumns & offset_columns, size_t level)
{
	if (type.isNullable())
	{
		/// First write to the null map.
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const IDataType & nested_type = *nullable_type.getNestedType();

		const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(column);
		const IColumn & nested_col = *nullable_col.getNestedColumn();

		std::string filename = name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION;

		Mark mark;
		mark.rows = (storage.files[filename].marks.empty() ? 0 : storage.files[filename].marks.back().rows) + column.size();
		mark.offset = streams[filename]->plain_offset + streams[filename]->plain.count();

		out_null_marks.emplace_back(storage.files[filename].column_index, mark);

		DataTypeUInt8{}.serializeBinary(*nullable_col.getNullMapColumn(),
			streams[filename]->compressed);
		streams[filename]->compressed.next();

		/// Then write data.
		writeData(name, nested_type, nested_col, out_marks, out_null_marks, offset_columns, level);
	}
	else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
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

		writeData(name, *type_arr->getNestedType(), typeid_cast<const ColumnArray &>(column).getData(),
			out_marks, out_null_marks, offset_columns, level + 1);
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

void LogBlockOutputStream::writeMarks(MarksForColumns marks, bool write_null_marks)
{
	size_t count = write_null_marks ? storage.null_file_count : storage.file_count;
	WriteBufferFromFile & stream = write_null_marks ? *null_marks_stream : marks_stream;
	const Names & names = write_null_marks ? storage.null_map_filenames : storage.column_names;

	if (marks.size() != count)
		throw Exception("Wrong number of marks generated from block. Makes no sense.", ErrorCodes::LOGICAL_ERROR);

	sort(marks.begin(), marks.end(), ColumnIndexLess);

	for (size_t i = 0; i < marks.size(); ++i)
	{
		Mark mark = marks[i].second;

		writeIntBinary(mark.rows, stream);
		writeIntBinary(mark.offset, stream);

		size_t column_index = marks[i].first;
		storage.files[names[column_index]].marks.push_back(mark);
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
	file_checker(path + escapeForFileName(name) + '/' + "sizes.json")
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

	/// создаём файлы, если их нет
	Poco::File(path + escapeForFileName(name) + '/').createDirectories();

	for (const auto & column : getColumnsList())
		addFile(column.name, *column.type);

	marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_MARKS_FILE_NAME);

	if (has_nullable_columns)
		null_marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_NULL_MARKS_FILE_NAME);
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
	return make_shared(
		path_, name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_,
		max_compress_block_size_
	);
}

StoragePtr StorageLog::create(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	size_t max_compress_block_size_)
{
	return make_shared(
		path_, name_, columns_,
		NamesAndTypesList{}, NamesAndTypesList{}, ColumnDefaults{},
		max_compress_block_size_
	);
}


void StorageLog::addFile(const String & column_name, const IDataType & type, size_t level)
{
	if (files.end() != files.find(column_name))
		throw Exception("Duplicate column with name " + column_name + " in constructor of StorageLog.",
			ErrorCodes::DUPLICATE_COLUMN);

	if (type.isNullable())
	{
		/// First add the file describing the null map of the column.
		has_nullable_columns = true;

		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
		const IDataType & actual_type = *nullable_type.getNestedType();

		std::string filename = column_name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION;
		ColumnData & column_data = files.emplace(filename, ColumnData{}).first->second;
		++null_file_count;
		column_data.column_index = null_map_filenames.size();
		column_data.data_file = Poco::File{
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name)
			+ DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION};

		null_map_filenames.push_back(filename);

		/// Then add the file describing the column data.
		addFile(column_name, actual_type, level);
	}
	else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_column_suffix = ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		String size_name = DataTypeNested::extractNestedTableName(column_name) + size_column_suffix;

		if (files.end() == files.find(size_name))
		{
			ColumnData & column_data = files.insert(std::make_pair(size_name, ColumnData())).first->second;
			++file_count;
			column_data.column_index = column_names.size();
			column_data.data_file = Poco::File{
				path + escapeForFileName(name) + '/'
				+ escapeForFileName(DataTypeNested::extractNestedTableName(column_name))
				+ size_column_suffix + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION};

			column_names.push_back(size_name);
		}

		addFile(column_name, *type_arr->getNestedType(), level + 1);
	}
	else
	{
		ColumnData & column_data = files.insert(std::make_pair(column_name, ColumnData())).first->second;
		++file_count;
		column_data.column_index = column_names.size();
		column_data.data_file = Poco::File{
			path + escapeForFileName(name) + '/'
			+ escapeForFileName(column_name) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION};

		column_names.push_back(column_name);
	}
}


void StorageLog::loadMarks()
{
	Poco::ScopedWriteRWLock lock(rwlock);

	if (loaded_marks)
		return;

	loadMarksImpl(false);
	if (has_nullable_columns)
		loadMarksImpl(true);

	loaded_marks = true;
}


void StorageLog::loadMarksImpl(bool load_null_marks)
{
	using FilesByIndex = std::vector<Files_t::iterator>;

	size_t count = load_null_marks ? null_file_count : file_count;
	Poco::File & marks_file_handle = load_null_marks ? null_marks_file : marks_file;

	FilesByIndex files_by_index(count);
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
	{
		bool has_null_extension = endsWith(it->first, DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION);
		if (!load_null_marks && has_null_extension)
			continue;
		if (load_null_marks && !has_null_extension)
			continue;

		files_by_index[it->second.column_index] = it;
	}

	if (marks_file_handle.exists())
	{
		size_t file_size = marks_file_handle.getSize();
		if (file_size % (count * sizeof(Mark)) != 0)
			throw Exception("Size of marks file is inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);

		int marks_count = file_size / (count * sizeof(Mark));

		for (size_t i = 0; i < files_by_index.size(); ++i)
			files_by_index[i]->second.marks.reserve(marks_count);

		ReadBufferFromFile marks_rb(marks_file_handle.path(), 32768);
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
		it->second.data_file = Poco::File(path + escapeForFileName(name) + '/' + Poco::Path(it->second.data_file.path()).getFileName());

	marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_MARKS_FILE_NAME);
	if (has_nullable_columns)
		null_marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_NULL_MARKS_FILE_NAME);
}


const Marks & StorageLog::getMarksWithRealRowCount() const
{
	auto init_column_type = [&]()
	{
		const IDataType * type = columns->front().type.get();
		if (type->isNullable())
		{
			const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*type);
			type = nullable_type.getNestedType().get();
		}
		return type;
	};

	const String & column_name = columns->front().name;
	const IDataType & column_type = *init_column_type();
	String filename;

	/** Засечки достаём из первого столбца.
	  * Если это - массив, то берём засечки, соответствующие размерам, а не внутренностям массивов.
	  */

	if (typeid_cast<const DataTypeArray *>(&column_type))
		filename = DataTypeNested::extractNestedTableName(column_name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX "0";
	else
		filename = column_name;

	Files_t::const_iterator it = files.find(filename);
	if (files.end() == it)
		throw Exception("Cannot find file " + filename, ErrorCodes::LOGICAL_ERROR);

	return it->second.marks;
}


BlockInputStreams StorageLog::read(
	size_t from_mark,
	size_t to_mark,
	size_t from_null_mark,
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	/** Если читаем все данные в один поток, то засечки не требуются.
	  * Отсутствие необходимости загружать засечки позволяет уменьшить потребление памяти.
	  */
	bool read_all_data_in_one_thread = (threads == 1 && from_mark == 0 && to_mark == std::numeric_limits<size_t>::max());
	if (!read_all_data_in_one_thread)
		loadMarks();

	check(column_names);

	processed_stage = QueryProcessingStage::FetchColumns;

	Poco::ScopedReadRWLock lock(rwlock);

	BlockInputStreams res;

	if (read_all_data_in_one_thread)
	{
		res.push_back(std::make_shared<LogBlockInputStream>(
			max_block_size,
			column_names,
			*this,
			0, marksCount() ? std::numeric_limits<size_t>::max() : 0,
			settings.max_read_buffer_size));
	}
	else
	{
		const Marks & marks = getMarksWithRealRowCount();
		size_t marks_size = marks.size();

		/// Given a thread, return the start of the area from which
		/// it can read data, i.e. a mark number.
		auto mark_from_thread = [&](size_t thread)
		{
			/// The computation below reflects the fact that marks
			/// are uniformly distributed among threads.
			return from_mark + thread * (to_mark - from_mark) / threads;
		};

		/// Given a thread, get the parameters that specify the area
		/// from which it can read data, i.e. a mark number and a
		/// maximum number of rows.
		auto get_reader_parameters = [&](size_t thread)
		{
			size_t mark_number = mark_from_thread(thread);

			size_t cur_total_row_count = ((thread == 0 && from_mark == 0)
						? 0
						: marks[mark_number - 1].rows);
			size_t next_total_row_count = marks[mark_from_thread(thread + 1) - 1].rows;
			size_t rows_limit = next_total_row_count - cur_total_row_count;

			return std::make_pair(mark_number, rows_limit);
		};

		if (to_mark == std::numeric_limits<size_t>::max())
			to_mark = marks_size;

		if (to_mark > marks_size || to_mark < from_mark)
			throw Exception("Marks out of range in StorageLog::read", ErrorCodes::LOGICAL_ERROR);

		if (threads > to_mark - from_mark)
			threads = to_mark - from_mark;

		if (has_nullable_columns)
		{
			for (size_t thread = 0; thread < threads; ++thread)
			{
				size_t mark_number;
				size_t rows_limit;
				std::tie(mark_number, rows_limit) = get_reader_parameters(thread);

				/// This works since we have the same number of marks and null marks.
				size_t null_mark_number = from_null_mark + (mark_number - from_mark);

				res.push_back(std::make_shared<LogBlockInputStream>(
					max_block_size,
					column_names,
					*this,
					mark_number,
					null_mark_number,
					rows_limit,
					settings.max_read_buffer_size));
			}
		}
		else
		{
			for (size_t thread = 0; thread < threads; ++thread)
			{
				size_t mark_number;
				size_t rows_limit;
				std::tie(mark_number, rows_limit) = get_reader_parameters(thread);

				res.push_back(std::make_shared<LogBlockInputStream>(
					max_block_size,
					column_names,
					*this,
					mark_number,
					rows_limit,
					settings.max_read_buffer_size));
			}
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
	return read(
		0, std::numeric_limits<size_t>::max(),
		0,
		column_names,
		query, context, settings, processed_stage,
		max_block_size, threads);
}


BlockOutputStreamPtr StorageLog::write(
	ASTPtr query, const Settings & settings)
{
	loadMarks();
	return std::make_shared<LogBlockOutputStream>(*this);
}

bool StorageLog::checkData() const
{
	Poco::ScopedReadRWLock lock(const_cast<Poco::RWLock &>(rwlock));

	return file_checker.check();
}

}
