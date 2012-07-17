#pragma once

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>

#include <DB/Columns/ColumnsNumber.h>

#include <DB/Interpreters/sortBlock.h>

#include <DB/Storages/StorageMergeTree.h>


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
		const ColumnUInt16::Container_t & dates = dynamic_cast<const ColumnUInt16 &>(block.getByName(storage.date_column_name)).getData();

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
		for (BlocksByMonth::const_iterator it = blocks_by_month.begin(); it != blocks_by_month.end(); ++it)
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
		BlockWithDateInterval(Block & block, UInt16 min_date_, UInt16 max_date_) : min_date(min_date_), max_date(max_date_) {}
	};

	struct Stream
	{
		Stream(const std::string & data_path, const std::string & marks_path) :
			plain(data_path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY),
			compressed(plain),
			marks(marks_path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY) {}

		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;
		WriteBufferFromFile marks;
	};

	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;

	void writePart(Block & block, UInt16 min_date, UInt16 max_date)
	{
		size_t columns = block.columns();
		UInt64 part_id = storage.increment.get(true);

		String part_tmp_path = storage.full_path
			+ "tmp_"
			+ storage.getPartName(
				min_date, max_date,
				part_id, part_id, 0)
			+ "/";

		Poco::File(part_tmp_path).createDirectories();

		/// Если для сортировки надо вычислить некоторые столбцы - делаем это.
		storage.primary_expr.execute(block);

		/// Сортируем.
		sortBlock(block, storage.sort_descr);

		/// Теперь удаляем лишние (вычисленные только для сортировки) столбцы.
		while (block.columns() != columns)
			block.erase(columns);

		/// Наконец-то можно писать данные на диск.
		for (size_t i = 0; i < columns; ++i)
		{
			const ColumnWithNameAndType & column = block.getByPosition(i);

			Mark mark;
			mark.rows = (storage.files[column.name].marks.empty() ? 0 : storage.files[column.name].marks.back().rows) + column.column->size();
			mark.offset = streams[column.name]->plain.count();

			writeIntBinary(mark.rows, streams[column.name]->marks);
			writeIntBinary(mark.offset, streams[column.name]->marks);

			storage.files[column.name].marks.push_back(mark);

			column.type->serializeBinary(*column.column, streams[column.name]->compressed);
			streams[column.name]->compressed.next();
		}
	}
};


StorageMergeTree::StorageMergeTree(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_, const String & date_column_name_,
	size_t index_granularity_)
	: path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), columns(columns_),
	context(context_), primary_expr_ast(primary_expr_ast_->clone()),
	date_column_name(date_column_name_), index_granularity(index_granularity_),
	primary_expr(primary_expr_ast, context),
	increment(full_path + "increment.txt")
{
	/// создаём директорию, если её нет
	Poco::File(full_path).createDirectories();

	/// инициализируем описание сортировки
	sort_descr.reserve(primary_expr_ast->children.size());
	for (ASTs::iterator it = primary_expr_ast->children.begin();
		it != primary_expr_ast->children.end();
		++it)
	{
		String name = (*it)->children.front()->getColumnName();
		sort_descr.push_back(SortColumnDescription(name, 1));
	}
}


String StorageMergeTree::getPartName(Yandex::DayNum_t left_month, Yandex::DayNum_t right_month, UInt64 left_id, UInt64 right_id, UInt64 level)
{
	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
	
	/// Имя директории для куска иммет вид: YYYYMM_YYYYMM_N_N_0.
	String res;
	{
		unsigned min_y = date_lut.toYear(left_month);
		unsigned max_y = date_lut.toYear(right_month);
		unsigned min_m = date_lut.toMonth(left_month);
		unsigned max_m = date_lut.toMonth(right_month);

		WriteBufferFromString wb(res);

		writeIntText(min_y, wb);
		if (min_m < 10)
			writeChar('0', wb);
		writeIntText(min_m, wb);

		writeChar('_', wb);

		writeIntText(max_y, wb);
		if (max_m < 10)
			writeChar('0', wb);
		writeIntText(max_m, wb);

		writeChar('_', wb);
		writeIntText(left_id, wb);
		writeChar('_', wb);
		writeIntText(right_id, wb);
		writeChar('_', wb);
		writeIntText(level, wb);
	}

	return res;
}

}
