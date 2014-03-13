#include <boost/bind.hpp>
#include <numeric>

#include <Poco/DirectoryIterator.h>
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
#include <DB/Columns/ColumnNested.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeNested.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>
#include <DB/DataStreams/SummingSortedBlockInputStream.h>
#include <DB/DataStreams/CollapsingFinalBlockInputStream.h>
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
#include <DB/Parsers/ASTNameTypePair.h>

#include <DB/Interpreters/sortBlock.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <DB/Storages/MergeTree/MergedBlockOutputStream.h>

#include <algorithm>


namespace DB
{

static String lastTwoPathComponents(String path)
{
	if (!path.empty() && *path.rbegin() == '/')
		path.erase(path.end() - 1);
	size_t slash = path.rfind('/');
	if (slash == String::npos || slash == 0)
		return path;
	slash = path.rfind('/', slash - 1);
	if (slash == String::npos)
		return path;
	return path.substr(slash + 1);
}

MergeTreeData::MergeTreeData(
	const String & full_path_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
	: context(context_),
	date_column_name(date_column_name_), sampling_expression(sampling_expression_),
	index_granularity(index_granularity_),
	mode(mode_), sign_column(sign_column_),
	settings(settings_), primary_expr_ast(primary_expr_ast_->clone()),
	full_path(full_path_), columns(columns_),
	log(&Logger::get("MergeTreeData: " + lastTwoPathComponents(full_path))),
	file_name_regexp("^(\\d{8})_(\\d{8})_(\\d+)_(\\d+)_(\\d+)")
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

	primary_expr = ExpressionAnalyzer(primary_expr_ast, context, *columns).getActions(false);

	ExpressionActionsPtr projected_expr = ExpressionAnalyzer(primary_expr_ast, context, *columns).getActions(true);
	primary_key_sample = projected_expr->getSampleBlock();

	loadDataParts();
	clearOldParts();
}

UInt64 MergeTreeData::getMaxDataPartIndex()
{
	UInt64 max_part_id = 0;
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		max_part_id = std::max(max_part_id, (*it)->right);
	}
	return max_part_id;
}

std::string MergeTreeData::getModePrefix() const
{
	switch (mode)
	{
		case Ordinary: 		return "";
		case Collapsing: 	return "Collapsing";
		case Summing: 		return "Summing";

		default:
			throw Exception("Unknown mode of operation for MergeTreeData: " + toString(mode), ErrorCodes::LOGICAL_ERROR);
	}
}




String MergeTreeData::getPartName(DayNum_t left_date, DayNum_t right_date, UInt64 left_id, UInt64 right_id, UInt64 level)
{
	DateLUTSingleton & date_lut = DateLUTSingleton::instance();

	/// Имя директории для куска иммет вид: YYYYMMDD_YYYYMMDD_N_N_L.
	String res;
	{
		unsigned left_date_id = Date2OrderedIdentifier(date_lut.fromDayNum(left_date));
		unsigned right_date_id = Date2OrderedIdentifier(date_lut.fromDayNum(right_date));

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


void MergeTreeData::parsePartName(const String & file_name, const Poco::RegularExpression::MatchVec & matches, DataPart & part)
{
	DateLUTSingleton & date_lut = DateLUTSingleton::instance();

	part.left_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[1].offset, matches[1].length)));
	part.right_date = date_lut.toDayNum(OrderedIdentifier2Date(file_name.substr(matches[2].offset, matches[2].length)));
	part.left = parse<UInt64>(file_name.substr(matches[3].offset, matches[3].length));
	part.right = parse<UInt64>(file_name.substr(matches[4].offset, matches[4].length));
	part.level = parse<UInt32>(file_name.substr(matches[5].offset, matches[5].length));

	part.left_month = date_lut.toFirstDayNumOfMonth(part.left_date);
	part.right_month = date_lut.toFirstDayNumOfMonth(part.right_date);
}


void MergeTreeData::loadDataParts()
{
	LOG_DEBUG(log, "Loading data parts");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	data_parts.clear();

	Strings part_file_names;
	Strings old_file_names;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		String file_name = it.name();

		/// Удаляем временные директории старше суток.
		if (0 == file_name.compare(0, strlen("tmp_"), "tmp_"))
		{
			Poco::File tmp_dir(full_path + file_name);

			if (tmp_dir.isDirectory() && tmp_dir.getLastModified().epochTime() + 86400 < time(0))
			{
				LOG_WARNING(log, "Removing temporary directory " << full_path << file_name);
				Poco::File(full_path + file_name).remove(true);
			}

			continue;
		}

		if (0 == file_name.compare(0, strlen("old_"), "old_"))
			old_file_names.push_back(file_name);
		else
			part_file_names.push_back(file_name);
	}

	Poco::RegularExpression::MatchVec matches;
	while (!part_file_names.empty())
	{
		String file_name = part_file_names.back();
		part_file_names.pop_back();

		if (!isPartDirectory(file_name, matches))
			continue;

		DataPartPtr part = new DataPart(*this);
		parsePartName(file_name, matches, *part);
		part->name = file_name;

		/// Для битых кусков, которые могут образовываться после грубого перезапуска сервера, попытаться восстановить куски, из которых они сделаны.
		if (isBrokenPart(full_path + file_name))
		{
			if (part->level == 0)
			{
				/// Восстановить куски нулевого уровня невозможно.
				LOG_ERROR(log, "Removing broken part " << full_path + file_name << " because is't impossible to repair.");
				part->remove();
			}
			else
			{
				Strings new_parts = tryRestorePart(full_path, file_name, old_file_names);
				part_file_names.insert(part_file_names.begin(), new_parts.begin(), new_parts.end());
			}

			continue;
		}

		/// Размер - в количестве засечек.
		part->size = Poco::File(full_path + file_name + "/" + escapeForFileName(columns->front().first) + ".mrk").getSize()
			/ MERGE_TREE_MARK_SIZE;

		part->modification_time = Poco::File(full_path + file_name).getLastModified().epochTime();

		try
		{
			part->loadIndex();
		}
		catch (...)
		{
			/// Не будем вставлять в набор кусок с битым индексом. Пропустим кусок и позволим серверу запуститься.
			tryLogCurrentException(__PRETTY_FUNCTION__);
			continue;
		}

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


void MergeTreeData::clearOldParts()
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
		if (ref_count == 1)		/// После этого ref_count не может увеличиться.
		{
			LOG_DEBUG(log, "'Removing' part " << (*it)->name << " (prepending old_ to its name)");

			(*it)->renameToOld();
			all_data_parts.erase(it++);
		}
		else
			++it;
	}

	/// Удалим старые old_ куски.
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		if (0 != it.name().compare(0, strlen("old_"), "old_"))
			continue;
		if (it->isDirectory() && it->getLastModified().epochTime() + settings.old_parts_lifetime < time(0))
		{
			it->remove(true);
		}
	}
}

void MergeTreeData::setPath(const String & new_full_path)
{
	Poco::File(full_path).renameTo(new_full_path);
	full_path = new_full_path;

	context.getUncompressedCache()->reset();
	context.getMarkCache()->reset();

	log = &Logger::get(lastTwoPathComponents(full_path));
}

void MergeTreeData::dropAllData()
{
	data_parts.clear();
	all_data_parts.clear();

	context.getUncompressedCache()->reset();
	context.getMarkCache()->reset();

	Poco::File(full_path).remove(true);
}

void MergeTreeData::removeColumnFiles(String column_name)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	/// Регэксп выбирает файлы столбца для удаления
	Poco::RegularExpression re(column_name + "(?:(?:\\.|\\%2E).+){0,1}" +"(?:\\.mrk|\\.bin|\\.size\\d+\\.bin|\\.size\\d+\\.mrk)");
	/// Цикл по всем директориям кусочков
	Poco::RegularExpression::MatchVec matches;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it_dir = Poco::DirectoryIterator(full_path); it_dir != end; ++it_dir)
	{
		std::string dir_name = it_dir.name();

		if (!isPartDirectory(dir_name, matches))
			continue;

		/// Цикл по каждому из файлов в директории кусочков
		String full_dir_name = full_path + dir_name + "/";
		for (Poco::DirectoryIterator it_file(full_dir_name); it_file != end; ++it_file)
		{
			if (re.match(it_file.name()))
			{
				Poco::File file(full_dir_name + it_file.name());
				if (file.exists())
					file.remove();
			}
		}
	}
}

void MergeTreeData::createConvertExpression(const String & in_column_name, const String & out_type, ExpressionActionsPtr & out_expression, String & out_column)
{
	ASTFunction * function = new ASTFunction;
	ASTPtr function_ptr = function;

	ASTExpressionList * arguments = new ASTExpressionList;
	ASTPtr arguments_ptr = arguments;

	function->name = "to" + out_type;
	function->arguments = arguments_ptr;
	function->children.push_back(arguments_ptr);

	ASTIdentifier * in_column = new ASTIdentifier;
	ASTPtr in_column_ptr = in_column;

	arguments->children.push_back(in_column_ptr);

	in_column->name = in_column_name;
	in_column->kind = ASTIdentifier::Column;

	out_expression = ExpressionAnalyzer(function_ptr, context, *columns).getActions(false);
	out_column = function->getColumnName();
}

void MergeTreeData::alter(const ASTAlterQuery::Parameters & params)
{
	/*if (params.type == ASTAlterQuery::MODIFY)
	{
		{
			typedef std::vector<DataPartPtr> PartsList;
			PartsList parts;
			{
				Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
				for (auto & data_part : data_parts)
				{
					parts.push_back(data_part);
				}
			}

			Names column_name;
			const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = name_type.type->range;
			String type(type_range.first, type_range.second - type_range.first);
			DB::DataTypePtr old_type_ptr = getDataTypeByName(name_type.name);
			DB::DataTypePtr new_type_ptr = context.getDataTypeFactory().get(type);
			if (dynamic_cast<DataTypeNested *>(old_type_ptr.get()) || dynamic_cast<DataTypeArray *>(old_type_ptr.get()) ||
				dynamic_cast<DataTypeNested *>(new_type_ptr.get()) || dynamic_cast<DataTypeArray *>(new_type_ptr.get()))
				throw DB::Exception("ALTER MODIFY not supported for nested and array types");

			column_name.push_back(name_type.name);
			DB::ExpressionActionsPtr expr;
			String out_column;
			createConvertExpression(name_type.name, type, expr, out_column);

			ColumnNumbers num(1, 0);
			for (DataPartPtr & part : parts)
			{
				MarkRanges ranges(1, MarkRange(0, part->size));
				ExpressionBlockInputStream in(new MergeTreeBlockInputStream(full_path + part->name + '/',
						DEFAULT_MERGE_BLOCK_SIZE, column_name, *this, part, ranges, StoragePtr(), false, NULL, ""), expr);
				MergedColumnOnlyOutputStream out(*this, full_path + part->name + '/', true);
				out.writePrefix();

				try
				{
					while(DB::Block b = in.read())
					{
						/// оставляем только столбец с результатом
						b.erase(0);
						out.write(b);
					}
					LOG_TRACE(log, "Write Suffix");
					out.writeSuffix();
				}
				catch (const Exception & e)
				{
					if (e.code() != ErrorCodes::ALL_REQUESTED_COLUMNS_ARE_MISSING)
						throw;
				}
			}

			/// переименовываем файлы
			/// переименовываем старые столбцы, добавляя расширение .old
			for (DataPartPtr & part : parts)
			{
				std::string path = full_path + part->name + '/' + escapeForFileName(name_type.name);
				if (Poco::File(path + ".bin").exists())
				{
					LOG_TRACE(log, "Renaming " << path + ".bin" << " to " << path + ".bin" + ".old");
					Poco::File(path + ".bin").renameTo(path + ".bin" + ".old");
					LOG_TRACE(log, "Renaming " << path + ".mrk" << " to " << path + ".mrk" + ".old");
					Poco::File(path + ".mrk").renameTo(path + ".mrk" + ".old");
				}
			}

			/// переименовываем временные столбцы
			for (DataPartPtr & part : parts)
			{
				std::string path = full_path + part->name + '/' + escapeForFileName(out_column);
				std::string new_path = full_path + part->name + '/' + escapeForFileName(name_type.name);
				if (Poco::File(path + ".bin").exists())
				{
					LOG_TRACE(log, "Renaming " << path + ".bin" << " to " << new_path + ".bin");
					Poco::File(path + ".bin").renameTo(new_path + ".bin");
					LOG_TRACE(log, "Renaming " << path + ".mrk" << " to " << new_path + ".mrk");
					Poco::File(path + ".mrk").renameTo(new_path + ".mrk");
				}
			}

			// удаляем старые столбцы
			for (DataPartPtr & part : parts)
			{
				std::string path = full_path + part->name + '/' + escapeForFileName(name_type.name);
				if (Poco::File(path + ".bin" + ".old").exists())
				{
					LOG_TRACE(log, "Removing old column " << path + ".bin" + ".old");
					Poco::File(path + ".bin" + ".old").remove();
					LOG_TRACE(log, "Removing old column " << path + ".mrk" + ".old");
					Poco::File(path + ".mrk" + ".old").remove();
				}
			}
		}
		context.getUncompressedCache()->reset();
		context.getMarkCache()->reset();
	}
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
		alterColumns(params, columns, context);
	}
	if (params.type == ASTAlterQuery::DROP)
	{
		String column_name = dynamic_cast<const ASTIdentifier &>(*params.column).name;
		removeColumnFiles(column_name);
	}*/
}


bool MergeTreeData::isPartDirectory(const String & dir_name, Poco::RegularExpression::MatchVec & matches) const
{
	return (file_name_regexp.match(dir_name, 0, matches) && 6 == matches.size());
}


bool MergeTreeData::isBrokenPart(const String & path)
{
	/// Проверяем, что первичный ключ непуст.

	Poco::File index_file(path + "/primary.idx");

	if (!index_file.exists() || index_file.getSize() == 0)
	{
		LOG_ERROR(log, "Part " << path << " is broken: primary key is empty.");

		return true;
	}

	/// Проверяем, что все засечки непусты и имеют одинаковый размер.

	ssize_t marks_size = -1;
	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
	{
		Poco::File marks_file(path + "/" + escapeForFileName(it->first) + ".mrk");

		/// при добавлении нового столбца в таблицу файлы .mrk не создаются. Не будем ничего удалять.
		if (!marks_file.exists())
			continue;

		if (marks_size == -1)
		{
			marks_size = marks_file.getSize();

			if (0 == marks_size)
			{
				LOG_ERROR(log, "Part " << path << " is broken: " << marks_file.path() << " is empty.");

				return true;
			}
		}
		else
		{
			if (static_cast<ssize_t>(marks_file.getSize()) != marks_size)
			{
				LOG_ERROR(log, "Part " << path << " is broken: marks have different sizes.");

				return true;
			}
		}
	}

	return false;
}

Strings MergeTreeData::tryRestorePart(const String & path, const String & file_name, Strings & old_parts)
{
	LOG_ERROR(log, "Restoring all old_ parts covered by " << file_name);

	Poco::RegularExpression::MatchVec matches;
	Strings restored_parts;

	isPartDirectory(file_name, matches);
	DataPart broken_part(*this);
	parsePartName(file_name, matches, broken_part);

	for (int i = static_cast<int>(old_parts.size()) - 1; i >= 0; --i)
	{
		DataPart old_part(*this);
		String name = old_parts[i].substr(strlen("old_"));
		if (!isPartDirectory(name, matches))
		{
			LOG_ERROR(log, "Strange file name: " << path + old_parts[i] << "; ignoring");
			old_parts.erase(old_parts.begin() + i);
			continue;
		}
		parsePartName(name, matches, old_part);
		if (broken_part.contains(old_part))
		{
			/// Восстанавливаем все содержащиеся куски. Если некоторые из них содержатся в других, их удалит loadDataParts.
			LOG_ERROR(log, "Restoring part " << path + old_parts[i]);
			Poco::File(path + old_parts[i]).renameTo(path + name);
			old_parts.erase(old_parts.begin() + i);
			restored_parts.push_back(name);
		}
	}

	if (restored_parts.size() >= 2)
	{
		LOG_ERROR(log, "Removing broken part " << path + file_name << " because at least 2 old_ parts were restored in its place");
		Poco::File(path + file_name).remove(true);
	}
	else
	{
		LOG_ERROR(log, "Not removing broken part " << path + file_name
			<< " because less than 2 old_ parts were restored in its place. You need to resolve this manually");
	}

	return restored_parts;
}

void MergeTreeData::replaceParts(DataPartsVector old_parts, DataPartPtr new_part)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> all_lock(all_data_parts_mutex);

	for (size_t i = 0; i < old_parts.size(); ++i)
		if (data_parts.end() == data_parts.find(old_parts[i]))
			throw Exception("Logical error: cannot find data part " + old_parts[i]->name + " in list", ErrorCodes::LOGICAL_ERROR);

	data_parts.insert(new_part);
	all_data_parts.insert(new_part);

	for (size_t i = 0; i < old_parts.size(); ++i)
		data_parts.erase(data_parts.find(old_parts[i]));
}

void MergeTreeData::renameTempPartAndAdd(DataPartPtr part, Increment * increment,
		const MergeTreeData::LockedTableStructurePtr & structure)
{
	LOG_TRACE(log, "Renaming.");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	String old_path = structure->getFullPath() + part->name + "/";

	UInt64 part_id = part->left;

	/** Важно, что получение номера куска происходит атомарно с добавлением этого куска в набор.
	  * Иначе есть race condition - может произойти слияние пары кусков, диапазоны номеров которых
	  *  содержат ещё не добавленный кусок.
	  */
	if (increment)
		part_id = increment->get(false);

	part->left = part->right = part_id;
	part->name = getPartName(part->left_date, part->right_date, part_id, part_id, 0);

	String new_path = structure->getFullPath() + part->name + "/";

	/// Переименовываем кусок.
	Poco::File(old_path).renameTo(new_path);

	data_parts.insert(part);
	all_data_parts.insert(part);
}

MergeTreeData::DataParts MergeTreeData::getDataParts()
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	return data_parts;
}

}
