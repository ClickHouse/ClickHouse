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
#include <DB/IO/WriteBufferFromFile.h>
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
	const String & log_name_,
	bool require_part_metadata_)
	: context(context_),
	date_column_name(date_column_name_), sampling_expression(sampling_expression_),
	index_granularity(index_granularity_),
	mode(mode_), sign_column(sign_column_),
	settings(settings_), primary_expr_ast(primary_expr_ast_->clone()),
	require_part_metadata(require_part_metadata_),
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
			part->loadColumns();
			part->loadChecksums();
			part->loadIndex();
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


void MergeTreeData::checkAlter(const AlterCommands & params)
{
	/// Проверим, что указанные преобразования можно совершить над списком столбцов без учета типов.
	NamesAndTypesList new_columns = *columns;
	params.apply(new_columns);

	/// Список столбцов, которые нельзя трогать.
	/// sampling_expression можно не учитывать, потому что он обязан содержаться в первичном ключе.
	Names keys = primary_expr->getRequiredColumns();
	keys.push_back(sign_column);
	std::sort(keys.begin(), keys.end());

	for (const AlterCommand & command : params)
	{
		if (std::binary_search(keys.begin(), keys.end(), command.column_name))
			throw Exception("trying to ALTER key column " + command.column_name, ErrorCodes::ILLEGAL_COLUMN);
	}

	/// Проверим, что преобразования типов возможны.
	ExpressionActionsPtr unused_expression;
	NameToNameMap unused_map;
	createConvertExpression(nullptr, *columns, new_columns, unused_expression, unused_map);
}

void MergeTreeData::createConvertExpression(DataPartPtr part, const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns,
	ExpressionActionsPtr & out_expression, NameToNameMap & out_rename_map)
{
	out_expression = nullptr;
	out_rename_map.clear();

	typedef std::map<String, DataTypePtr> NameToType;
	NameToType new_types;
	for (const NameAndTypePair & column : new_columns)
	{
		new_types[column.name] = column.type;
	}

	/// Сколько столбцов сейчас в каждой вложенной структуре. Столбцы не из вложенных структур сюда тоже попадут и не помешают.
	std::map<String, int> nested_table_counts;
	for (const NameAndTypePair & column : old_columns)
	{
		++nested_table_counts[DataTypeNested::extractNestedTableName(column.name)];
	}

	for (const NameAndTypePair & column : old_columns)
	{
		if (!new_types.count(column.name))
		{
			if (!part || part->hasColumnFiles(column.name))
			{
				/// Столбец нужно удалить.

				String escaped_column = escapeForFileName(column.name);
				out_rename_map[escaped_column + ".bin"] = "";
				out_rename_map[escaped_column + ".mrk"] = "";

				/// Если это массив или последний столбец вложенной структуры, нужно удалить файлы с размерами.
				if (typeid_cast<const DataTypeArray *>(&*column.type))
				{
					String nested_table = DataTypeNested::extractNestedTableName(column.name);
					/// Если это был последний столбец, относящийся к этим файлам .size0, удалим файлы.
					if (!--nested_table_counts[nested_table])
					{
						String escaped_nested_table = escapeForFileName(nested_table);
						out_rename_map[escaped_nested_table + ".size0.bin"] = "";
						out_rename_map[escaped_nested_table + ".size0.mrk"] = "";
					}
				}
			}
		}
		else
		{
			String new_type_name = new_types[column.name]->getName();

			if (new_type_name != column.type->getName() &&
				(!part || part->hasColumnFiles(column.name)))
			{
				/// Нужно изменить тип столбца.

				if (!out_expression)
					out_expression = new ExpressionActions(NamesAndTypesList(), context.getSettingsRef());

				out_expression->addInput(ColumnWithNameAndType(nullptr, column.type, column.name));

				FunctionPtr function = context.getFunctionFactory().get("to" + new_type_name, context);
				Names out_names;
				out_expression->add(ExpressionAction::applyFunction(function, Names(1, column.name)), out_names);
				out_expression->add(ExpressionAction::removeColumn(column.name));

				String escaped_expr = escapeForFileName(out_names[0]);
				String escaped_column = escapeForFileName(column.name);
				out_rename_map[escaped_expr + ".bin"] = escaped_column + ".bin";
				out_rename_map[escaped_expr + ".mrk"] = escaped_column + ".mrk";
			}
		}
	}
}

MergeTreeData::AlterDataPartTransactionPtr MergeTreeData::alterDataPart(DataPartPtr part, const NamesAndTypesList & new_columns)
{
	ExpressionActionsPtr expression;
	AlterDataPartTransactionPtr transaction(new AlterDataPartTransaction(part));
	createConvertExpression(part, part->columns, new_columns, expression, transaction->rename_map);

	if (transaction->rename_map.empty())
	{
		transaction->clear();
		return transaction;
	}

	DataPart::Checksums add_checksums;

	/// Применим выражение и запишем результат во временные файлы.
	if (expression)
	{
		MarkRanges ranges(1, MarkRange(0, part->size));
		BlockInputStreamPtr part_in = new MergeTreeBlockInputStream(full_path + part->name + '/',
			DEFAULT_MERGE_BLOCK_SIZE, expression->getRequiredColumns(), *this, part, ranges, false, nullptr, "");
		ExpressionBlockInputStream in(part_in, expression);
		MergedColumnOnlyOutputStream out(*this, full_path + part->name + '/', true);
		in.readPrefix();
		out.writePrefix();

		while (Block b = in.read())
			out.write(b);

		in.readSuffix();
		add_checksums = out.writeSuffixAndGetChecksums();
	}

	/// Обновим контрольные суммы.
	DataPart::Checksums new_checksums = part->checksums;
	for (auto it : transaction->rename_map)
	{
		if (it.second == "")
		{
			new_checksums.files.erase(it.first);
		}
		else
		{
			new_checksums.files[it.second] = add_checksums.files[it.first];
		}
	}

	/// Запишем обновленные контрольные суммы во временный файл
	if (!part->checksums.empty())
	{
		transaction->new_checksums = new_checksums;
		WriteBufferFromFile checksums_file(full_path + part->name + "/checksums.txt.tmp", 4096);
		new_checksums.writeText(checksums_file);
		transaction->rename_map["checksums.txt.tmp"] = "checksums.txt";
	}

	/// Запишем обновленный список столбцов во временный файл.
	{
		transaction->new_columns = new_columns.filter(part->columns.getNames());
		WriteBufferFromFile columns_file(full_path + part->name + "/columns.txt.tmp", 4096);
		transaction->new_columns.writeText(columns_file);
		transaction->rename_map["columns.txt.tmp"] = "columns.txt";
	}

	return transaction;
}

void MergeTreeData::AlterDataPartTransaction::commit()
{
	if (!data_part)
		return;
	try
	{
		Poco::ScopedWriteRWLock lock(data_part->columns_lock);

		String path = data_part->storage.full_path + data_part->name + "/";

		/// 1) Переименуем старые файлы.
		for (auto it : rename_map)
		{
			String name = it.second.empty() ? it.first : it.second;
			Poco::File(path + name).renameTo(path + name + ".tmp2");
		}

		/// 2) Переместим на их место новые и обновим метаданные в оперативке.
		for (auto it : rename_map)
		{
			if (!it.second.empty())
			{
				Poco::File(path + it.first).renameTo(path + it.second);
			}
		}

		DataPart & mutable_part = const_cast<DataPart &>(*data_part);
		mutable_part.checksums = new_checksums;
		mutable_part.columns = new_columns;

		/// 3) Удалим старые файлы.
		for (auto it : rename_map)
		{
			String name = it.second.empty() ? it.first : it.second;
			Poco::File(path + name + ".tmp2").remove();
		}

		mutable_part.size_in_bytes = MergeTreeData::DataPart::calcTotalSize(path);

		clear();
	}
	catch (...)
	{
		/// Если что-то пошло не так, не будем удалять временные файлы в деструкторе.
		clear();
		throw;
	}
}

MergeTreeData::AlterDataPartTransaction::~AlterDataPartTransaction()
{
	try
	{
		if (!data_part)
			return;

		LOG_WARNING(data_part->storage.log, "Aborting ALTER of part " << data_part->name);

		String path = data_part->storage.full_path + data_part->name + "/";
		for (auto it : rename_map)
		{
			if (!it.second.empty())
			{
				try
				{
					Poco::File(path + it.first).remove();
				}
				catch (Poco::Exception & e)
				{
					LOG_WARNING(data_part->storage.log, "Can't remove " << path + it.first << ": " << e.displayText());
				}
			}
		}
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


void MergeTreeData::renameTempPartAndAdd(MutableDataPartPtr part, Increment * increment, Transaction * out_transaction)
{
	auto removed = renameTempPartAndReplace(part, increment, out_transaction);
	if (!removed.empty())
	{
		LOG_ERROR(log, "Added part " << part->name << + " covers " << toString(removed.size())
			<< " existing part(s) (including " << removed[0]->name << ")");
	}
}

MergeTreeData::DataPartsVector MergeTreeData::renameTempPartAndReplace(
	MutableDataPartPtr part, Increment * increment, Transaction * out_transaction)
{
	if (out_transaction && out_transaction->data)
		throw Exception("Using the same MergeTreeData::Transaction for overlapping transactions is invalid");

	LOG_TRACE(log, "Renaming " << part->name << ".");

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

	if (out_transaction)
	{
		out_transaction->data = this;
		out_transaction->added_parts = res;
		out_transaction->removed_parts = DataPartsVector(1, part);
	}

	return res;
}

void MergeTreeData::replaceParts(const DataPartsVector & remove, const DataPartsVector & add, bool clear_without_timeout)
{
	LOG_TRACE(log, "Removing " << remove.size() << " parts and adding " << add.size() << " parts.");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	for (const DataPartPtr & part : remove)
	{
		part->remove_time = clear_without_timeout ? 0 : time(0);
		data_parts.erase(part);
	}
	for (const DataPartPtr & part : add)
	{
		data_parts.insert(part);
	}
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

void MergeTreeData::deletePart(DataPartPtr part)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	data_parts.erase(part);
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
