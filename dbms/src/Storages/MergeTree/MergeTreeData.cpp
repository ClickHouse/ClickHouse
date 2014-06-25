#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <Yandex/time2str.h>
#include <Poco/Ext/ScopedTry.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Storages/MergeTree/MergeTreeReader.h>
#include <DB/Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <DB/Storages/MergeTree/MergedBlockOutputStream.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/copyData.h>
#include <algorithm>



namespace DB
{

MergeTreeData::MergeTreeData(
	const String & full_path_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_,
	const String & log_name_)
	: context(context_),
	date_column_name(date_column_name_), sampling_expression(sampling_expression_),
	index_granularity(index_granularity_),
	mode(mode_), sign_column(sign_column_),
	settings(settings_), primary_expr_ast(primary_expr_ast_->clone()),
	full_path(full_path_), columns(columns_), log_name(log_name_),
	log(&Logger::get(log_name + " (Data)"))
{
	/// создаём директорию, если её нет
	Poco::File(full_path).createDirectories();

	/// инициализируем описание сортировки
	sort_descr.reserve(primary_expr_ast->children.size());
	for (const ASTPtr & ast : primary_expr_ast->children)
	{
		String name = ast->getColumnName();
		sort_descr.push_back(SortColumnDescription(name, 1));
	}

	primary_expr = ExpressionAnalyzer(primary_expr_ast, context, *columns).getActions(false);

	ExpressionActionsPtr projected_expr = ExpressionAnalyzer(primary_expr_ast, context, *columns).getActions(true);
	primary_key_sample = projected_expr->getSampleBlock();

	loadDataParts();
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
		case Aggregating: 	return "Aggregating";

		default:
			throw Exception("Unknown mode of operation for MergeTreeData: " + toString(mode), ErrorCodes::LOGICAL_ERROR);
	}
}


void MergeTreeData::loadDataParts()
{
	LOG_DEBUG(log, "Loading data parts");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	data_parts.clear();

	Strings all_file_names;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
		all_file_names.push_back(it.name());

	Strings part_file_names;
	for (const String & file_name : all_file_names)
	{
		/// Удаляем временные директории старше суток.
		if (0 == file_name.compare(0, strlen("tmp_"), "tmp_"))
			continue;

		if (0 == file_name.compare(0, strlen("old_"), "old_"))
		{
			String new_file_name = file_name.substr(strlen("old_"));
			LOG_WARNING(log, "Renaming " << file_name << " to " << new_file_name << " for compatibility reasons");
			Poco::File(full_path + file_name).renameTo(full_path + new_file_name);
			part_file_names.push_back(new_file_name);
		}
		else
		{
			part_file_names.push_back(file_name);
		}
	}

	DataPartsVector broken_parts_to_remove;

	Poco::RegularExpression::MatchVec matches;
	for (const String & file_name : part_file_names)
	{
		if (!ActiveDataPartSet::isPartDirectory(file_name, matches))
			continue;

		MutableDataPartPtr part = std::make_shared<DataPart>(*this);
		ActiveDataPartSet::parsePartName(file_name, *part, &matches);
		part->name = file_name;

		bool broken = false;

		try
		{
			part->loadIndex();
			part->loadChecksums();
			part->checkNotBroken();
		}
		catch (...)
		{
			broken = true;
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		/// Игнорируем и, возможно, удаляем битые куски, которые могут образовываться после грубого перезапуска сервера.
		if (broken)
		{
			if (part->level == 0)
			{
				/// Восстановить куски нулевого уровня невозможно.
				LOG_ERROR(log, "Removing broken part " << full_path + file_name << " because is't impossible to repair.");
				broken_parts_to_remove.push_back(part);
			}
			else
			{
				/// Посмотрим, сколько кусков покрыты битым. Если хотя бы два, предполагаем, что битый кусок образован их
				///  слиянием, и мы ничего не потеряем, если его удалим.
				int contained_parts = 0;

				LOG_ERROR(log, "Part " << full_path + file_name << " is broken. Looking for parts to replace it.");

				for (const String & contained_name : part_file_names)
				{
					if (contained_name == file_name)
						continue;
					if (!ActiveDataPartSet::isPartDirectory(contained_name, matches))
						continue;
					DataPart contained_part(*this);
					ActiveDataPartSet::parsePartName(contained_name, contained_part, &matches);
					if (part->contains(contained_part))
					{
						LOG_ERROR(log, "Found part " << full_path + contained_name);
						++contained_parts;
					}
				}

				if (contained_parts >= 2)
				{
					LOG_ERROR(log, "Removing broken part " << full_path + file_name << " because it covers at least 2 other parts");
					broken_parts_to_remove.push_back(part);
				}
				else
				{
					LOG_ERROR(log, "Not removing broken part " << full_path + file_name
						<< " because it covers less than 2 parts. You need to resolve this manually");
				}
			}

			continue;
		}

		part->modification_time = Poco::File(full_path + file_name).getLastModified().epochTime();

		data_parts.insert(part);
	}

	if (broken_parts_to_remove.size() > 2)
		throw Exception("Suspiciously many (" + toString(broken_parts_to_remove.size()) + ") broken parts to remove.",
			ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);

	for (const auto & part : broken_parts_to_remove)
		part->remove();

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
				(*prev_jt)->remove_time = time(0);
				data_parts.erase(prev_jt);
				prev_jt = curr_jt;
				++curr_jt;
			}
			else if ((*prev_jt)->contains(**curr_jt))
			{
				(*curr_jt)->remove_time = time(0);
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


Strings MergeTreeData::clearOldParts()
{
	Poco::ScopedTry<Poco::FastMutex> lock;
	Strings res;

	/// Если метод уже вызван из другого потока (или если all_data_parts прямо сейчас меняют), то можно ничего не делать.
	if (!lock.lock(&all_data_parts_mutex))
	{
		return res;
	}

	time_t now = time(0);
	for (DataParts::iterator it = all_data_parts.begin(); it != all_data_parts.end();)
	{
		int ref_count = it->use_count();
		if (ref_count == 1 && /// После этого ref_count не может увеличиться.
			(*it)->remove_time + settings.old_parts_lifetime < now)
		{
			LOG_DEBUG(log, "Removing part " << (*it)->name);

			res.push_back((*it)->name);
			(*it)->remove();
			all_data_parts.erase(it++);
		}
		else
			++it;
	}

	/// Удаляем временные директории старше суток.
	Strings all_file_names;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
		all_file_names.push_back(it.name());

	for (const String & file_name : all_file_names)
	{
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
	}

	return res;
}

void MergeTreeData::setPath(const String & new_full_path)
{
	Poco::File(full_path).renameTo(new_full_path);
	full_path = new_full_path;

	context.resetCaches();
}

void MergeTreeData::dropAllData()
{
	data_parts.clear();
	all_data_parts.clear();

	context.resetCaches();

	Poco::File(full_path).remove(true);
}

void MergeTreeData::removeColumnFiles(String column_name, bool remove_array_size_files)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	size_t dot_pos = column_name.find('.');
	if (dot_pos != std::string::npos)
	{
		std::string nested_column = column_name.substr(0, dot_pos);
		column_name = nested_column + "%2E" + column_name.substr(dot_pos + 1);

		if (remove_array_size_files)
			column_name = std::string("(?:") + nested_column + "|" + column_name + ")";
	}

	/// Регэксп выбирает файлы столбца для удаления
	Poco::RegularExpression re(column_name + "(?:(?:\\.|\\%2E).+){0,1}" +"(?:\\.mrk|\\.bin|\\.size\\d+\\.bin|\\.size\\d+\\.mrk)");
	/// Цикл по всем директориям кусочков
	Poco::RegularExpression::MatchVec matches;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it_dir = Poco::DirectoryIterator(full_path); it_dir != end; ++it_dir)
	{
		std::string dir_name = it_dir.name();

		if (!ActiveDataPartSet::isPartDirectory(dir_name, matches))
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

		/// Удаляем лишние столбцы из checksums.txt
		for (auto & part : all_data_parts)
		{
			if (!part)
				continue;

			for (auto it = part->checksums.files.lower_bound(column_name);
				(it != part->checksums.files.end()) && (it->first.substr(0, column_name.size()) == column_name); ++it)
			{
				if (re.match(it->first))
					const_cast<DataPart::Checksums::FileChecksums &>(part->checksums.files).erase(it);
			}
			/// Записываем файл с чексуммами.
			WriteBufferFromFile out(full_path + part->name + "/" + "checksums.txt", 1024);
			part->checksums.writeText(out);
		}
	}
}

void MergeTreeData::createConvertExpression(
	const String & in_column_name,
	const String & out_type,
	ExpressionActionsPtr & out_expression,
	String & out_column)
{
	Names out_names;
	out_expression = new ExpressionActions(
		NamesAndTypesList(1, NameAndTypePair(in_column_name, getDataTypeByName(in_column_name))), context.getSettingsRef());

	FunctionPtr function = context.getFunctionFactory().get("to" + out_type, context);
	out_expression->add(ExpressionAction::applyFunction(function, Names(1, in_column_name)), out_names);
	out_expression->add(ExpressionAction::removeColumn(in_column_name));

	out_column = out_names[0];
}

static DataTypePtr getDataTypeByName(const String & name, const NamesAndTypesList & columns)
{
	for (const auto & it : columns)
	{
		if (it.first == name)
			return it.second;
	}
	throw Exception("No column " + name + " in table", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}

/// одинаковыми считаются имена, вида "name.*"
static bool namesWithDotEqual(const String & name_with_dot, const NameAndTypePair & name_type)
{
	return (name_with_dot == name_type.first.substr(0, name_with_dot.length()));
}

void MergeTreeData::alter(const ASTAlterQuery::Parameters & params)
{
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
		alterColumns(params, columns, context);
	}

	if (params.type == ASTAlterQuery::DROP)
	{
		String column_name = dynamic_cast<const ASTIdentifier &>(*params.column).name;

		/// Если нет колонок вида nested_name.*, то удалим столбцы размера массивов
		bool remove_array_size_files = false;
		size_t dot_pos = column_name.find('.');
		if (dot_pos != std::string::npos)
		{
			remove_array_size_files = (columns->end() == std::find_if(columns->begin(), columns->end(), boost::bind(namesWithDotEqual, column_name.substr(0, dot_pos), _1)));
		}
		removeColumnFiles(column_name, remove_array_size_files);

		context.resetCaches();
	}
}

void MergeTreeData::prepareAlterModify(const ASTAlterQuery::Parameters & params)
{
	DataPartsVector parts;
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		parts = DataPartsVector(data_parts.begin(), data_parts.end());
	}

	Names column_name;
	const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
	StringRange type_range = name_type.type->range;
	String type(type_range.first, type_range.second - type_range.first);
	DataTypePtr old_type_ptr = DB::getDataTypeByName(name_type.name, *columns);
	DataTypePtr new_type_ptr = context.getDataTypeFactory().get(type);
	if (dynamic_cast<DataTypeNested *>(old_type_ptr.get()) || dynamic_cast<DataTypeArray *>(old_type_ptr.get()) ||
		dynamic_cast<DataTypeNested *>(new_type_ptr.get()) || dynamic_cast<DataTypeArray *>(new_type_ptr.get()))
		throw Exception("ALTER MODIFY not supported for nested and array types");

	column_name.push_back(name_type.name);
	ExpressionActionsPtr expr;
	String out_column;
	createConvertExpression(name_type.name, type, expr, out_column);

	ColumnNumbers num(1, 0);
	for (DataPartPtr & part : parts)
	{
		MarkRanges ranges(1, MarkRange(0, part->size));
		ExpressionBlockInputStream in(new MergeTreeBlockInputStream(full_path + part->name + '/',
			DEFAULT_MERGE_BLOCK_SIZE, column_name, *this, part, ranges, false, nullptr, ""), expr);
		MergedColumnOnlyOutputStream out(*this, full_path + part->name + '/', true);
		in.readPrefix();
		out.writePrefix();

		try
		{
			while (Block b = in.read())
				out.write(b);

			in.readSuffix();
			DataPart::Checksums add_checksums = out.writeSuffixAndGetChecksums();

			/// Запишем обновленные контрольные суммы во временный файл.
			if (!part->checksums.empty())
			{
				DataPart::Checksums new_checksums = part->checksums;
				std::string escaped_name = escapeForFileName(name_type.name);
				std::string escaped_out_column = escapeForFileName(out_column);
				new_checksums.files[escaped_name  + ".bin"] = add_checksums.files[escaped_out_column + ".bin"];
				new_checksums.files[escaped_name  + ".mrk"] = add_checksums.files[escaped_out_column + ".mrk"];

				WriteBufferFromFile checksums_file(full_path + part->name + '/' + escaped_out_column + ".checksums.txt", 1024);
				new_checksums.writeText(checksums_file);
			}
		}
		catch (const Exception & e)
		{
			if (e.code() != ErrorCodes::ALL_REQUESTED_COLUMNS_ARE_MISSING)
				throw;
		}
	}
}

void MergeTreeData::commitAlterModify(const ASTAlterQuery::Parameters & params)
{
	DataPartsVector parts;
	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		parts = DataPartsVector(data_parts.begin(), data_parts.end());
	}

	const ASTNameTypePair & name_type = dynamic_cast<const ASTNameTypePair &>(*params.name_type);
	StringRange type_range = name_type.type->range;
	String type(type_range.first, type_range.second - type_range.first);

	ExpressionActionsPtr expr;
	String out_column;
	createConvertExpression(name_type.name, type, expr, out_column);

	/// переименовываем файлы
	/// переименовываем старые столбцы, добавляя расширение .old
	for (DataPartPtr & part : parts)
	{
		std::string part_path = full_path + part->name + '/';
		std::string path = part_path + escapeForFileName(name_type.name);
		if (Poco::File(path + ".bin").exists())
		{
			LOG_TRACE(log, "Renaming " << path + ".bin" << " to " << path + ".bin" + ".old");
			Poco::File(path + ".bin").renameTo(path + ".bin" + ".old");
			LOG_TRACE(log, "Renaming " << path + ".mrk" << " to " << path + ".mrk" + ".old");
			Poco::File(path + ".mrk").renameTo(path + ".mrk" + ".old");

			if (Poco::File(part_path + "checksums.txt").exists())
			{
				LOG_TRACE(log, "Renaming " << part_path + "checksums.txt" << " to " << part_path + "checksums.txt" + ".old");
				Poco::File(part_path + "checksums.txt").renameTo(part_path + "checksums.txt" + ".old");
			}
		}
	}

	/// переименовываем временные столбцы
	for (DataPartPtr & part : parts)
	{
		std::string part_path = full_path + part->name + '/';
		std::string name = escapeForFileName(out_column);
		std::string new_name = escapeForFileName(name_type.name);
		std::string path = part_path + name;
		std::string new_path = part_path + new_name;
		if (Poco::File(path + ".bin").exists())
		{
			LOG_TRACE(log, "Renaming " << path + ".bin" << " to " << new_path + ".bin");
			Poco::File(path + ".bin").renameTo(new_path + ".bin");
			LOG_TRACE(log, "Renaming " << path + ".mrk" << " to " << new_path + ".mrk");
			Poco::File(path + ".mrk").renameTo(new_path + ".mrk");

			if (Poco::File(path + ".checksums.txt").exists())
			{
				LOG_TRACE(log, "Renaming " << path + ".checksums.txt" << " to " << part_path + ".checksums.txt");
				Poco::File(path + ".checksums.txt").renameTo(part_path + "checksums.txt");
			}
		}
	}

	// удаляем старые столбцы
	for (DataPartPtr & part : parts)
	{
		std::string part_path = full_path + part->name + '/';
		std::string path = part_path + escapeForFileName(name_type.name);
		if (Poco::File(path + ".bin" + ".old").exists())
		{
			LOG_TRACE(log, "Removing old column " << path + ".bin" + ".old");
			Poco::File(path + ".bin" + ".old").remove();
			LOG_TRACE(log, "Removing old column " << path + ".mrk" + ".old");
			Poco::File(path + ".mrk" + ".old").remove();

			if (Poco::File(part_path + "checksums.txt" + ".old").exists())
			{
				LOG_TRACE(log, "Removing old checksums " << part_path + "checksums.txt" + ".old");
				Poco::File(part_path + "checksums.txt" + ".old").remove();
			}
		}
	}

	context.resetCaches();

	{
		Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
		alterColumns(params, columns, context);
	}
}


void MergeTreeData::renameTempPartAndAdd(MutableDataPartPtr part, Increment * increment)
{
	auto removed = renameTempPartAndReplace(part, increment);
	if (!removed.empty())
	{
		LOG_ERROR(log, "Added part " << part->name << + " covers " << toString(removed.size())
			<< " existing part(s) (including " << removed[0]->name << ")");
	}
}

MergeTreeData::DataPartsVector MergeTreeData::renameTempPartAndReplace(MutableDataPartPtr part, Increment * increment)
{
	LOG_TRACE(log, "Renaming.");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	String old_path = getFullPath() + part->name + "/";

	/** Для StorageMergeTree важно, что получение номера куска происходит атомарно с добавлением этого куска в набор.
	  * Иначе есть race condition - может произойти слияние пары кусков, диапазоны номеров которых
	  *  содержат ещё не добавленный кусок.
	  */
	if (increment)
		part->left = part->right = increment->get(false);

	part->name = ActiveDataPartSet::getPartName(part->left_date, part->right_date, part->left, part->right, part->level);

	if (data_parts.count(part))
		throw Exception("Part " + part->name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);

	String new_path = getFullPath() + part->name + "/";

	/// Переименовываем кусок.
	Poco::File(old_path).renameTo(new_path);

	bool obsolete = false; /// Покрыт ли part каким-нибудь куском.
	DataPartsVector res;
	/// Куски, содержащиеся в part, идут в data_parts подряд, задевая место, куда вставился бы сам part.
	DataParts::iterator it = data_parts.lower_bound(part);
	/// Пойдем влево.
	while (it != data_parts.begin())
	{
		--it;
		if (!part->contains(**it))
		{
			if ((*it)->contains(*part))
				obsolete = true;
			++it;
			break;
		}
		res.push_back(*it);
		(*it)->remove_time = time(0);
		data_parts.erase(it++); /// Да, ++, а не --.
	}
	std::reverse(res.begin(), res.end()); /// Нужно получить куски в порядке возрастания.
	/// Пойдем вправо.
	while (it != data_parts.end())
	{
		if (!part->contains(**it))
		{
			if ((*it)->name == part->name || (*it)->contains(*part))
				obsolete = true;
			break;
		}
		res.push_back(*it);
		(*it)->remove_time = time(0);
		data_parts.erase(it++);
	}

	if (obsolete)
	{
		LOG_WARNING(log, "Obsolete part " + part->name + " added");
	}
	else
	{
		data_parts.insert(part);
	}

	all_data_parts.insert(part);

	return res;
}

void MergeTreeData::renameAndDetachPart(DataPartPtr part, const String & prefix)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);
	if (!all_data_parts.erase(part))
		throw Exception("No such data part", ErrorCodes::NO_SUCH_DATA_PART);
	data_parts.erase(part);
	part->renameAddPrefix(prefix);
}

MergeTreeData::DataParts MergeTreeData::getDataParts()
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	return data_parts;
}

MergeTreeData::DataParts MergeTreeData::getAllDataParts()
{
	Poco::ScopedLock<Poco::FastMutex> lock(all_data_parts_mutex);

	return all_data_parts;
}

size_t MergeTreeData::getMaxPartsCountForMonth()
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	size_t res = 0;
	size_t cur_count = 0;
	DayNum_t cur_month = DayNum_t(0);

	for (const auto & part : data_parts)
	{
		if (part->left_month == cur_month)
		{
			++cur_count;
		}
		else
		{
			cur_month = part->left_month;
			cur_count = 1;
		}

		res = std::max(res, cur_count);
	}

	return res;
}

void MergeTreeData::delayInsertIfNeeded()
{
	size_t parts_count = getMaxPartsCountForMonth();
	if (parts_count > settings.parts_to_delay_insert)
	{
		double delay = std::pow(settings.insert_delay_step, parts_count - settings.parts_to_delay_insert);
		delay /= 1000;
		delay = std::min(delay, DBMS_MAX_DELAY_OF_INSERT);

		LOG_INFO(log, "Delaying inserting block by "
			<< std::fixed << std::setprecision(4) << delay << "s because there are " << parts_count << " parts");
		std::this_thread::sleep_for(std::chrono::duration<double>(delay));
	}
}

MergeTreeData::DataPartPtr MergeTreeData::getContainingPart(const String & part_name, bool including_inactive)
{
	MutableDataPartPtr tmp_part(new DataPart(*this));
	ActiveDataPartSet::parsePartName(part_name, *tmp_part);

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	if (including_inactive)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(all_data_parts_mutex);
		DataParts::iterator it = all_data_parts.lower_bound(tmp_part);
		if (it != all_data_parts.end() && (*it)->name == part_name)
			return *it;
	}

	/// Кусок может покрываться только предыдущим или следующим в data_parts.
	DataParts::iterator it = data_parts.lower_bound(tmp_part);

	if (it != data_parts.end())
	{
		if ((*it)->name == part_name)
			return *it;
		if ((*it)->contains(*tmp_part))
			return *it;
	}

	if (it != data_parts.begin())
	{
		--it;
		if ((*it)->contains(*tmp_part))
			return *it;
	}

	return nullptr;
}


void MergeTreeData::DataPart::Checksums::Checksum::checkEqual(const Checksum & rhs, bool have_uncompressed, const String & name) const
{
	if (is_compressed && have_uncompressed)
	{
		if (!rhs.is_compressed)
			throw Exception("No uncompressed checksum for file " + name, ErrorCodes::CHECKSUM_DOESNT_MATCH);
		if (rhs.uncompressed_size != uncompressed_size)
			throw Exception("Unexpected size of file " + name + " in data part", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
		if (rhs.uncompressed_hash != uncompressed_hash)
			throw Exception("Checksum mismatch for file " + name + " in data part", ErrorCodes::CHECKSUM_DOESNT_MATCH);
		return;
	}
	if (rhs.file_size != file_size)
		throw Exception("Unexpected size of file " + name + " in data part", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
	if (rhs.file_hash != file_hash)
		throw Exception("Checksum mismatch for file " + name + " in data part", ErrorCodes::CHECKSUM_DOESNT_MATCH);
}

void MergeTreeData::DataPart::Checksums::Checksum::checkSize(const String & path) const
{
	Poco::File file(path);
	if (!file.exists())
		throw Exception(path + " doesn't exist", ErrorCodes::FILE_DOESNT_EXIST);
	size_t size = file.getSize();
	if (size != file_size)
		throw Exception(path + " has unexpected size: " + DB::toString(size) + " instead of " + DB::toString(file_size),
			ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
}

void MergeTreeData::DataPart::Checksums::checkEqual(const Checksums & rhs, bool have_uncompressed) const
{
	for (const auto & it : rhs.files)
	{
		const String & name = it.first;

		if (!files.count(name))
			throw Exception("Unexpected file " + name + " in data part", ErrorCodes::UNEXPECTED_FILE_IN_DATA_PART);
	}

	for (const auto & it : files)
	{
		const String & name = it.first;

		auto jt = rhs.files.find(name);
		if (jt == rhs.files.end())
			throw Exception("No file " + name + " in data part", ErrorCodes::NO_FILE_IN_DATA_PART);

		it.second.checkEqual(jt->second, have_uncompressed, name);
	}
}

void MergeTreeData::DataPart::Checksums::checkSizes(const String & path) const
{
	for (const auto & it : files)
	{
		const String & name = it.first;
		it.second.checkSize(path + name);
	}
}

bool MergeTreeData::DataPart::Checksums::readText(ReadBuffer & in)
{
	files.clear();
	size_t count;

	DB::assertString("checksums format version: ", in);
	int format_version;
	DB::readText(format_version, in);
	if (format_version < 1 || format_version > 2)
		throw Exception("Bad checksums format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT);
	if (format_version == 1)
		return false;
	DB::assertString("\n", in);
	DB::readText(count, in);
	DB::assertString(" files:\n", in);

	for (size_t i = 0; i < count; ++i)
	{
		String name;
		Checksum sum;

		DB::readString(name, in);
		DB::assertString("\n\tsize: ", in);
		DB::readText(sum.file_size, in);
		DB::assertString("\n\thash: ", in);
		DB::readText(sum.file_hash.first, in);
		DB::assertString(" ", in);
		DB::readText(sum.file_hash.second, in);
		DB::assertString("\n\tcompressed: ", in);
		DB::readText(sum.is_compressed, in);
		if (sum.is_compressed)
		{
			DB::assertString("\n\tuncompressed size: ", in);
			DB::readText(sum.uncompressed_size, in);
			DB::assertString("\n\tuncompressed hash: ", in);
			DB::readText(sum.uncompressed_hash.first, in);
			DB::assertString(" ", in);
			DB::readText(sum.uncompressed_hash.second, in);
		}
		DB::assertString("\n", in);

		files.insert(std::make_pair(name, sum));
	}

	return true;
}

void MergeTreeData::DataPart::Checksums::writeText(WriteBuffer & out) const
{
	DB::writeString("checksums format version: 2\n", out);
	DB::writeText(files.size(), out);
	DB::writeString(" files:\n", out);

	for (const auto & it : files)
	{
		const String & name = it.first;
		const Checksum & sum = it.second;
		DB::writeString(name, out);
		DB::writeString("\n\tsize: ", out);
		DB::writeText(sum.file_size, out);
		DB::writeString("\n\thash: ", out);
		DB::writeText(sum.file_hash.first, out);
		DB::writeString(" ", out);
		DB::writeText(sum.file_hash.second, out);
		DB::writeString("\n\tcompressed: ", out);
		DB::writeText(sum.is_compressed, out);
		DB::writeString("\n", out);
		if (sum.is_compressed)
		{
			DB::writeString("\tuncompressed size: ", out);
			DB::writeText(sum.uncompressed_size, out);
			DB::writeString("\n\tuncompressed hash: ", out);
			DB::writeText(sum.uncompressed_hash.first, out);
			DB::writeString(" ", out);
			DB::writeText(sum.uncompressed_hash.second, out);
			DB::writeString("\n", out);
		}
	}
}

}
