#include <boost/bind.hpp>

#include <Poco/DirectoryIterator.h>
#include <Poco/NumberParser.h>

#include <Yandex/time2str.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>

#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/sortBlock.h>

#include <DB/Storages/StorageMergeTree.h>


#define MERGE_TREE_MARK_SIZE (2 * sizeof(size_t))


namespace DB
{

class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	MergeTreeBlockOutputStream(StorageMergeTree & storage_) : storage(storage_)
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

		UInt16 min_month = date_lut.toFirstDayOfMonth(Yandex::DayNum_t(min_date));
		UInt16 max_month = date_lut.toFirstDayOfMonth(Yandex::DayNum_t(max_date));

		/// Типичный случай - когда месяц один (ничего разделять не нужно).
		if (min_month == max_month)
			blocks_by_month[min_month] = BlockWithDateInterval(block, min_date, max_date);
		else
		{
			for (size_t i = 0; i < rows; ++i)
			{
				UInt16 month = date_lut.toFirstDayOfMonth(dates[i]);
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
		size_t rows = block.rows();
		size_t columns = block.columns();
		UInt64 part_id = storage.increment.get(true);

		String part_tmp_path = storage.full_path
			+ "tmp_"
			+ storage.getPartName(
				Yandex::DayNum_t(min_date), Yandex::DayNum_t(max_date),
				part_id, part_id, 0)
			+ "/";

		String part_res_path = storage.full_path
			+ storage.getPartName(
				Yandex::DayNum_t(min_date), Yandex::DayNum_t(max_date),
				part_id, part_id, 0)
			+ "/";

		Poco::File(part_tmp_path).createDirectories();

		/// Если для сортировки надо вычислить некоторые столбцы - делаем это.
		storage.primary_expr->execute(block);

		/// Сортируем.
		sortBlock(block, storage.sort_descr);

		/// Наконец-то можно писать данные на диск.
		int flags = O_EXCL | O_CREAT | O_WRONLY;

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
		
		for (size_t i = 0; i < columns; ++i)
		{
			const ColumnWithNameAndType & column = block.getByPosition(i);
			String escaped_column_name = escapeForFileName(column.name);

			WriteBufferFromFile plain(part_tmp_path + escaped_column_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
			WriteBufferFromFile marks(part_tmp_path + escaped_column_name + ".mrk", DBMS_DEFAULT_BUFFER_SIZE, flags);
			CompressedWriteBuffer compressed(plain);

			size_t prev_mark = 0;
			column.type->serializeBinary(*column.column, compressed,
				boost::bind(&MergeTreeBlockOutputStream::writeCallback, this,
					boost::ref(prev_mark), boost::ref(plain), boost::ref(compressed), boost::ref(marks)));
		}

		/// Переименовываем кусок.
		Poco::File(part_tmp_path).renameTo(part_res_path);
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


/// Для чтения из одного куска. Для чтения сразу из многих, Storage использует сразу много таких объектов.
class MergeTreeBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergeTreeBlockInputStream(const String & path_,	/// Путь к куску
		size_t block_size_, const Names & column_names_, StorageMergeTree & storage_,
		size_t mark_number_, size_t rows_limit_)
		: path(path_), block_size(block_size_), column_names(column_names_),
		storage(storage_), mark_number(mark_number_), rows_limit(rows_limit_), rows_read(0)
	{
	}

	Block readImpl()
	{
		Block res;

		if (rows_read == rows_limit)
			return res;

		/// Если файлы не открыты, то открываем их.
		if (streams.empty())
			for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
				streams.insert(std::make_pair(*it, new Stream(
					path + escapeForFileName(*it),
					mark_number)));

		/// Сколько строк читать для следующего блока.
		size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);

		for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
		{
			String column_path = escapeForFileName(*it);
			ReadBufferFromFile plain(path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
			CompressedReadBuffer compressed(plain);
			
			ColumnWithNameAndType column;
			column.name = *it;
			column.type = storage.getDataTypeByName(*it);
			column.column = column.type->createColumn();
			column.type->deserializeBinary(*column.column, streams[column.name]->compressed, max_rows_to_read);

			if (column.column->size())
				res.insert(column);
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
	
	String getName() const { return "MergeTreeBlockInputStream"; }
	BlockInputStreamPtr clone() { return new MergeTreeBlockInputStream(path, block_size, column_names, storage, mark_number, rows_limit); }

private:
	const String path;
	size_t block_size;
	Names column_names;
	StorageMergeTree & storage;
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
};


StorageMergeTree::StorageMergeTree(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_, const String & date_column_name_,
	size_t index_granularity_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), columns(columns_),
	context(context_), primary_expr_ast(primary_expr_ast_->clone()),
	date_column_name(date_column_name_), index_granularity(index_granularity_),
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

	context.columns = *columns;
	primary_expr = new Expression(primary_expr_ast, context);

	loadDataParts();
}


BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query)
{
	return new MergeTreeBlockOutputStream(*this);
}


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
	bool in(const Field & x)
	{
		return (left_bounded
				? (boost::apply_visitor(FieldVisitorGreater(), x, left) || (left_included && x == left))
				: true)
			&& (right_bounded
				? (boost::apply_visitor(FieldVisitorLess(), x, right) || (right_included && x == right))
				: true);
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


/** Получить значение константного аргумента функции вида f(name, const_expr) или f(const_expr, name).
  * block_with_constants содержит вычисленные значения константных выражений.
  * Вернуть false, если такого нет.
  */
static bool getConstantArgument(ASTs & args, Block & block_with_constants, Field & rhs)
{
	if (args.size() != 2)
		return false;
	
	IAST * arg_rhs;

	if (dynamic_cast<ASTIdentifier *>(&*args[0]))
		arg_rhs = &*args[1];
	else if (dynamic_cast<ASTIdentifier *>(&*args[1]))
		arg_rhs = &*args[0];
	else
		return false;

	String rhs_column_name = arg_rhs->getColumnName();

	ASTLiteral * lit_rhs;
	if ((lit_rhs = dynamic_cast<ASTLiteral *>(arg_rhs)))
	{
		/// rhs - литерал
		rhs = lit_rhs->value;
		return true;
	}
	else if (block_with_constants.has(rhs_column_name) && block_with_constants.getByName(rhs_column_name).column->isConst())
	{
		/// rhs - выражение, вычислившееся в константу
		rhs = (*block_with_constants.getByName(rhs_column_name).column)[0];
		return true;
	}
	else
		return false;
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
		ASTIdentifier * ident;

		bool inverted;
		if ((ident = dynamic_cast<ASTIdentifier *>(&*args[0])))
			inverted = false;
		else if ((ident = dynamic_cast<ASTIdentifier *>(&*args[1])))
			inverted = true;
		else
			continue;

		if (ident->getColumnName() != column_name)
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

		ASTIdentifier * ident;
		if (!((ident = dynamic_cast<ASTIdentifier *>(&*args[0])) || (ident = dynamic_cast<ASTIdentifier *>(&*args[1]))))
			continue;

		if (ident->getColumnName() != column_name)
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
		LOG_DEBUG(log, "Primary key column " << sort_descr[primary_prefix.size()].column_name << " range: " << primary_range.toString());
	}
}


BlockInputStreams StorageMergeTree::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	/// Определим, можно ли использовать индексы, и как именно.

	/// Диапазон дат.
	Range date_range;
	/// Префикс первичного ключа, для которого требуется равенство. Может быть пустым.
	Row primary_prefix;
	/// Диапазон следующего после префикса столбца первичного ключа.
	Range primary_range;

	getIndexRanges(query, date_range, primary_prefix, primary_range);

	// TODO
	return BlockInputStreams();
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
	
	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
	SharedPtr<DataParts> new_data_parts = new DataParts;

	static Poco::RegularExpression file_name_regexp("^(\\d{8})_(\\d{8})_(\\d+)_(\\d+)_(\\d+)");
	Poco::DirectoryIterator end;
	Poco::RegularExpression::MatchVec matches;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		std::string file_name = it.name();

		if (!(file_name_regexp.match(file_name, 0, matches) && 6 == matches.size()))
			continue;
			
		DataPart part;
		part.left_date = date_lut.toDayNum(Yandex::OrderedIdentifier2Date(file_name.substr(matches[1].offset, matches[1].length)));
		part.right_date = date_lut.toDayNum(Yandex::OrderedIdentifier2Date(file_name.substr(matches[2].offset, matches[2].length)));
		part.left = Poco::NumberParser::parseUnsigned64(file_name.substr(matches[3].offset, matches[3].length));
		part.right = Poco::NumberParser::parseUnsigned64(file_name.substr(matches[4].offset, matches[4].length));
		part.level = Poco::NumberParser::parseUnsigned(file_name.substr(matches[5].offset, matches[5].length));
		part.name = file_name;

		/// Размер - в количестве засечек.
		part.size = Poco::File(full_path + file_name + "/" + escapeForFileName(columns->front().first) + ".mrk").getSize()
			/ MERGE_TREE_MARK_SIZE;
			
		part.modification_time = it->getLastModified().epochTime();

		part.left_month = date_lut.toFirstDayOfMonth(part.left_date);
		part.right_month = date_lut.toFirstDayOfMonth(part.right_date);

		new_data_parts->insert(part);
	}

	data_parts.set(new_data_parts);
	
	LOG_DEBUG(log, "Loaded data parts (" << new_data_parts->size() << " items)");
}

}
