#include <map>

#include <Poco/Path.h>
#include <Poco/Util/XMLConfiguration.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/Common/Exception.h>

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

#include <DB/Storages/StorageTinyLog.h>
#include <Poco/DirectoryIterator.h>

#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION 	".bin"


namespace DB
{

namespace ErrorCodes
{
	extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
	extern const int CANNOT_CREATE_DIRECTORY;
	extern const int CANNOT_READ_ALL_DATA;
	extern const int DUPLICATE_COLUMN;
}


class TinyLogBlockInputStream : public IProfilingBlockInputStream
{
public:
	TinyLogBlockInputStream(size_t block_size_, const Names & column_names_, StorageTinyLog & storage_, size_t max_read_buffer_size_)
		: block_size(block_size_), column_names(column_names_), column_types(column_names.size()),
		storage(storage_), max_read_buffer_size(max_read_buffer_size_) {}

	String getName() const { return "TinyLog"; }

	String getID() const;

protected:
	Block readImpl();
private:
	size_t block_size;
	Names column_names;
	DataTypes column_types;
	StorageTinyLog & storage;
	bool finished = false;
	size_t max_read_buffer_size;

	struct Stream
	{
		Stream(const std::string & data_path, size_t max_read_buffer_size)
			: plain(data_path, std::min(max_read_buffer_size, Poco::File(data_path).getSize())),
			compressed(plain)
		{
		}

		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;
	};

	using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;
	FileStreams streams;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void readData(const String & name, const IDataType & type, IColumn & column, size_t limit, size_t level = 0, bool read_offsets = true);
};


class TinyLogBlockOutputStream : public IBlockOutputStream
{
public:
	TinyLogBlockOutputStream(StorageTinyLog & storage_)
		: storage(storage_)
	{
		for (const auto & col : storage.getColumnsList())
			addStream(col.name, *col.type);
	}

	~TinyLogBlockOutputStream()
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
	StorageTinyLog & storage;
	bool done = false;

	struct Stream
	{
		Stream(const std::string & data_path, size_t max_compress_block_size) :
			plain(data_path, max_compress_block_size, O_APPEND | O_CREAT | O_WRONLY),
			compressed(plain, CompressionMethod::LZ4, max_compress_block_size)
		{
		}

		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;

		void finalize()
		{
			compressed.next();
			plain.next();
		}
	};

	using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;
	FileStreams streams;

	using OffsetColumns = std::set<std::string>;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns, size_t level = 0);
};


String TinyLogBlockInputStream::getID() const
{
	std::stringstream res;
	res << "TinyLog(" << storage.getTableName() << ", " << &storage;

	for (const auto & name : column_names)
		res << ", " << name;

	res << ")";
	return res.str();
}


Block TinyLogBlockInputStream::readImpl()
{
	Block res;

	if (finished || (!streams.empty() && streams.begin()->second->compressed.eof()))
	{
		/** Закрываем файлы (ещё до уничтожения объекта).
		  * Чтобы при создании многих источников, но одновременном чтении только из нескольких,
		  *  буферы не висели в памяти.
		  */
		finished = true;
		streams.clear();
		return res;
	}

	{
		/// если в папке нет файлов, то это значит, что таблица пока пуста
		if (Poco::DirectoryIterator(storage.full_path()) == Poco::DirectoryIterator())
			return res;
	}

	/// Если файлы не открыты, то открываем их.
	if (streams.empty())
	{
		for (size_t i = 0, size = column_names.size(); i < size; ++i)
		{
			const auto & name = column_names[i];
			column_types[i] = storage.getDataTypeByName(name);
			addStream(name, *column_types[i]);
		}
	}

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

		/// Для вложенных структур запоминаем указатели на столбцы со смещениями
		if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&*column.type))
		{
			String nested_name = DataTypeNested::extractNestedTableName(column.name);

			if (offset_columns.count(nested_name) == 0)
				offset_columns[nested_name] = std::make_shared<ColumnArray::ColumnOffsets_t>();
			else
				read_offsets = false; /// на предыдущих итерациях смещения уже считали вызовом readData

			column.column = std::make_shared<ColumnArray>(type_arr->getNestedType()->createColumn(), offset_columns[nested_name]);
		}
		else
			column.column = column.type->createColumn();

		try
		{
			readData(name, *column.type, *column.column, block_size, 0, read_offsets);
		}
		catch (Exception & e)
		{
			e.addMessage("while reading column " + name + " at " + storage.full_path());
			throw;
		}

		if (column.column->size())
			res.insert(std::move(column));
	}

	if (!res || streams.begin()->second->compressed.eof())
	{
		finished = true;
		streams.clear();
	}

	return res;
}


void TinyLogBlockInputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		if (!streams.count(size_name))
			streams.emplace(size_name, std::unique_ptr<Stream>(new Stream(storage.files[size_name].data_file.path(), max_read_buffer_size)));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams[name].reset(new Stream(storage.files[name].data_file.path(), max_read_buffer_size));
}


void TinyLogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t limit, size_t level, bool read_offsets)
{
	/// Для массивов требуется сначала десериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		if (read_offsets)
		{
			type_arr->deserializeOffsets(
				column,
				streams[DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level)]->compressed,
				limit);
		}

		if (column.size())
		{
			IColumn & nested_column = typeid_cast<ColumnArray &>(column).getData();
			size_t nested_limit = typeid_cast<ColumnArray &>(column).getOffsets()[column.size() - 1];
			readData(name, *type_arr->getNestedType(), nested_column, nested_limit, level + 1);

			if (nested_column.size() != nested_limit)
				throw Exception("Cannot read array data for all offsets", ErrorCodes::CANNOT_READ_ALL_DATA);
		}
	}
	else
		type.deserializeBinary(column, streams[name]->compressed, limit, 0);	/// TODO Использовать avg_value_size_hint.
}


void TinyLogBlockOutputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		if (!streams.count(size_name))
			streams.emplace(size_name, std::unique_ptr<Stream>(new Stream(storage.files[size_name].data_file.path(), storage.max_compress_block_size)));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams[name].reset(new Stream(storage.files[name].data_file.path(), storage.max_compress_block_size));
}


void TinyLogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column,
											OffsetColumns & offset_columns, size_t level)
{
	/// Для массивов требуется сначала сериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

		if (offset_columns.count(size_name) == 0)
		{
			offset_columns.insert(size_name);
			type_arr->serializeOffsets(
				column,
				streams[size_name]->compressed);
		}

		writeData(name, *type_arr->getNestedType(), typeid_cast<const ColumnArray &>(column).getData(), offset_columns, level + 1);
	}
	else
		type.serializeBinary(column, streams[name]->compressed);
}


void TinyLogBlockOutputStream::writeSuffix()
{
	if (done)
		return;
	done = true;

	/// Заканчиваем запись.
	for (FileStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		it->second->finalize();

	std::vector<Poco::File> column_files;
	for (auto & pair : streams)
		column_files.push_back(storage.files[pair.first].data_file);

	storage.file_checker.update(column_files.begin(), column_files.end());

	streams.clear();
}


void TinyLogBlockOutputStream::write(const Block & block)
{
	storage.check(block, true);

	/// Множество записанных столбцов со смещениями, чтобы не писать общие для вложенных структур столбцы несколько раз
	OffsetColumns offset_columns;

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithTypeAndName & column = block.getByPosition(i);
		writeData(column.name, *column.type, *column.column, offset_columns);
	}
}


StorageTinyLog::StorageTinyLog(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	bool attach,
	size_t max_compress_block_size_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	path(path_), name(name_), columns(columns_),
	max_compress_block_size(max_compress_block_size_),
	file_checker(path + escapeForFileName(name) + '/' + "sizes.json"),
	log(&Logger::get("StorageTinyLog"))
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageTinyLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

	String full_path = path + escapeForFileName(name) + '/';
	if (!attach)
	{
		/// создаём файлы, если их нет
		if (0 != mkdir(full_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) && errno != EEXIST)
			throwFromErrno("Cannot create directory " + full_path, ErrorCodes::CANNOT_CREATE_DIRECTORY);
	}

	for (const auto & col : getColumnsList())
		addFile(col.name, *col.type);
}

StoragePtr StorageTinyLog::create(
	const std::string & path_,
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	bool attach,
	size_t max_compress_block_size_)
{
	return make_shared(
		path_, name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_,
		attach, max_compress_block_size_
	);
}


void StorageTinyLog::addFile(const String & column_name, const IDataType & type, size_t level)
{
	if (files.end() != files.find(column_name))
		throw Exception("Duplicate column with name " + column_name + " in constructor of StorageTinyLog.",
			ErrorCodes::DUPLICATE_COLUMN);

	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		String size_column_suffix = ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
		String size_name = DataTypeNested::extractNestedTableName(column_name) + size_column_suffix;

		if (files.end() == files.find(size_name))
		{
			ColumnData column_data;
			files.insert(std::make_pair(size_name, column_data));
			files[size_name].data_file = Poco::File(
				path + escapeForFileName(name) + '/' + escapeForFileName(DataTypeNested::extractNestedTableName(column_name)) + size_column_suffix + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
		}

		addFile(column_name, *type_arr->getNestedType(), level + 1);
	}
	else
	{
		ColumnData column_data;
		files.insert(std::make_pair(column_name, column_data));
		files[column_name].data_file = Poco::File(
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
	}
}


void StorageTinyLog::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
	/// Переименовываем директорию с данными.
	Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_table_name));

	path = new_path_to_db;
	name = new_table_name;
	file_checker.setPath(path + escapeForFileName(name) + "/" + "sizes.json");

	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
		it->second.data_file = Poco::File(path + escapeForFileName(name) + '/' + Poco::Path(it->second.data_file.path()).getFileName());
}


BlockInputStreams StorageTinyLog::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;
	return BlockInputStreams(1, std::make_shared<TinyLogBlockInputStream>(max_block_size, column_names, *this, settings.max_read_buffer_size));
}


BlockOutputStreamPtr StorageTinyLog::write(
	ASTPtr query, const Settings & settings)
{
	return std::make_shared<TinyLogBlockOutputStream>(*this);
}


void StorageTinyLog::drop()
{
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
		if (it->second.data_file.exists())
			it->second.data_file.remove();
}

bool StorageTinyLog::checkData() const
{
	return file_checker.check();
}

}
