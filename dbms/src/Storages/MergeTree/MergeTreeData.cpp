#include <Poco/Ext/ScopedTry.h>

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Storages/MergeTree/MergeTreeReader.h>
#include <DB/Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <DB/Storages/MergeTree/MergedBlockOutputStream.h>
#include <DB/Storages/MergeTree/MergeTreePartChecker.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/Common/localBackup.h>
#include <DB/Functions/FunctionFactory.h>
#include <Poco/DirectoryIterator.h>
#include <statdaemons/Increment.h>

#include <algorithm>
#include <iomanip>
#include <thread>



namespace DB
{

MergeTreeData::MergeTreeData(
	const String & full_path_, NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_, const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	Mode mode_,
	const String & sign_column_,
	const Names & columns_to_sum_,
	const MergeTreeSettings & settings_,
	const String & log_name_,
	bool require_part_metadata_,
	BrokenPartCallback broken_part_callback_)
    : ITableDeclaration{materialized_columns_, alias_columns_, column_defaults_}, context(context_),
	date_column_name(date_column_name_), sampling_expression(sampling_expression_),
	index_granularity(index_granularity_),
	mode(mode_), sign_column(sign_column_), columns_to_sum(columns_to_sum_),
	settings(settings_), primary_expr_ast(primary_expr_ast_ ? primary_expr_ast_->clone() : nullptr),
	require_part_metadata(require_part_metadata_),
	full_path(full_path_), columns(columns_),
	broken_part_callback(broken_part_callback_),
	log_name(log_name_), log(&Logger::get(log_name + " (Data)"))
{
	/// Проверяем, что столбец с датой существует и имеет тип Date.
	const auto check_date_exists = [this] (const NamesAndTypesList & columns)
	{
		for (const auto & column : columns)
		{
			if (column.name == date_column_name)
			{
				if (!typeid_cast<const DataTypeDate *>(column.type.get()))
					throw Exception("Date column (" + date_column_name + ") for storage of MergeTree family must have type Date."
						" Provided column of type " + column.type->getName() + "."
						" You may have separate column with type " + column.type->getName() + ".", ErrorCodes::BAD_TYPE_OF_FIELD);
				return true;
			}
		}

		return false;
	};

	if (!check_date_exists(*columns) && !check_date_exists(materialized_columns))
		throw Exception{
			"Date column (" + date_column_name + ") does not exist in table declaration.",
			ErrorCodes::NO_SUCH_COLUMN_IN_TABLE};

	/// Если заданы columns_to_sum, проверяем, что такие столбцы существуют.
	if (!columns_to_sum.empty())
	{
		if (mode != Summing)
			throw Exception("List of columns to sum for MergeTree cannot be specified in all modes except Summing.", ErrorCodes::LOGICAL_ERROR);

		for (const auto & column_to_sum : columns_to_sum)
			if (columns->end() == std::find_if(columns->begin(), columns->end(),
				[&](const NameAndTypePair & name_and_type) { return column_to_sum == name_and_type.name; }))
				throw Exception("Column " + column_to_sum + " listed in columns to sum does not exist in table declaration.");
	}

	/// создаём директорию, если её нет
	Poco::File(full_path).createDirectories();
	Poco::File(full_path + "detached").createDirectory();

	if (primary_expr_ast)
	{
		/// инициализируем описание сортировки
		sort_descr.reserve(primary_expr_ast->children.size());
		for (const ASTPtr & ast : primary_expr_ast->children)
		{
			String name = ast->getColumnName();
			sort_descr.push_back(SortColumnDescription(name, 1));
		}

		primary_expr = ExpressionAnalyzer(primary_expr_ast, context, nullptr, getColumnsList()).getActions(false);

		ExpressionActionsPtr projected_expr = ExpressionAnalyzer(primary_expr_ast, context, nullptr, getColumnsList()).getActions(true);
		primary_key_sample = projected_expr->getSampleBlock();
	}
	else if (mode != Unsorted)
		throw Exception("Primary key could be empty only for UnsortedMergeTree", ErrorCodes::BAD_ARGUMENTS);
}

Int64 MergeTreeData::getMaxDataPartIndex()
{
	Int64 max_part_id = 0;
	for (const auto & part : data_parts)
		max_part_id = std::max(max_part_id, part->right);

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
		case Unsorted: 		return "Unsorted";

		default:
			throw Exception("Unknown mode of operation for MergeTreeData: " + toString(mode), ErrorCodes::LOGICAL_ERROR);
	}
}


void MergeTreeData::loadDataParts(bool skip_sanity_checks)
{
	LOG_DEBUG(log, "Loading data parts");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	data_parts.clear();

	Strings part_file_names;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		/// Пропускаем временные директории старше суток.
		if (0 == it.name().compare(0, strlen("tmp_"), "tmp_"))
			continue;

		part_file_names.push_back(it.name());
	}

	DataPartsVector broken_parts_to_remove;
	DataPartsVector broken_parts_to_detach;
	size_t suspicious_broken_parts = 0;

	Poco::RegularExpression::MatchVec matches;
	for (const String & file_name : part_file_names)
	{
		if (!ActiveDataPartSet::isPartDirectory(file_name, &matches))
			continue;

		MutableDataPartPtr part = std::make_shared<DataPart>(*this);
		ActiveDataPartSet::parsePartName(file_name, *part, &matches);
		part->name = file_name;

		bool broken = false;

		try
		{
			part->loadColumns(require_part_metadata);
			part->loadChecksums(require_part_metadata);
			part->loadIndex();
			part->checkNotBroken(require_part_metadata);
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
				LOG_ERROR(log, "Removing broken part " << full_path + file_name << " because it's impossible to repair.");
				broken_parts_to_remove.push_back(part);
			}
			else
			{
				/// Посмотрим, сколько кусков покрыты битым. Если хотя бы два, предполагаем, что битый кусок образован их
				///  слиянием, и мы ничего не потеряем, если его удалим.
				int contained_parts = 0;

				LOG_ERROR(log, "Part " << full_path + file_name << " is broken. Looking for parts to replace it.");
				++suspicious_broken_parts;

				for (const String & contained_name : part_file_names)
				{
					if (contained_name == file_name)
						continue;
					if (!ActiveDataPartSet::isPartDirectory(contained_name, &matches))
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
					LOG_ERROR(log, "Detaching broken part " << full_path + file_name
						<< " because it covers less than 2 parts. You need to resolve this manually");
					broken_parts_to_detach.push_back(part);
				}
			}

			continue;
		}

		part->modification_time = Poco::File(full_path + file_name).getLastModified().epochTime();

		data_parts.insert(part);
	}

	if (suspicious_broken_parts > settings.max_suspicious_broken_parts && !skip_sanity_checks)
		throw Exception("Suspiciously many (" + toString(suspicious_broken_parts) + ") broken parts to remove.",
			ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);

	for (const auto & part : broken_parts_to_remove)
		part->remove();
	for (const auto & part : broken_parts_to_detach)
		part->renameAddPrefix(true, "");

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
			if ((*curr_jt)->month != (*prev_jt)->month)
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

	calculateColumnSizes();

	LOG_DEBUG(log, "Loaded data parts (" << data_parts.size() << " items)");
}


MergeTreeData::DataPartsVector MergeTreeData::grabOldParts()
{
	Poco::ScopedTry<Poco::FastMutex> lock;
	DataPartsVector res;

	/// Если метод уже вызван из другого потока (или если all_data_parts прямо сейчас меняют), то можно ничего не делать.
	if (!lock.lock(&all_data_parts_mutex))
	{
		LOG_TRACE(log, "grabOldParts: all_data_parts is locked");
		return res;
	}

	/// Удаляем временные директории старше суток.
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it{full_path}; it != end; ++it)
	{
		if (0 == it.name().compare(0, strlen("tmp_"), "tmp_"))
		{
			Poco::File tmp_dir(full_path + it.name());

			if (tmp_dir.isDirectory() && tmp_dir.getLastModified().epochTime() + 86400 < time(0))
			{
				LOG_WARNING(log, "Removing temporary directory " << full_path << it.name());
				Poco::File(full_path + it.name()).remove(true);
			}
		}
	}

	time_t now = time(0);
	for (DataParts::iterator it = all_data_parts.begin(); it != all_data_parts.end();)
	{
		if (it->unique() && /// После этого ref_count не может увеличиться.
			(*it)->remove_time < now &&
			now - (*it)->remove_time > settings.old_parts_lifetime)
		{
			res.push_back(*it);
			all_data_parts.erase(it++);
		}
		else
			++it;
	}

	return res;
}

void MergeTreeData::addOldParts(const MergeTreeData::DataPartsVector & parts)
{
	Poco::ScopedLock<Poco::FastMutex> lock(all_data_parts_mutex);
	all_data_parts.insert(parts.begin(), parts.end());
}

void MergeTreeData::clearOldParts()
{
	auto parts_to_remove = grabOldParts();

	for (const DataPartPtr & part : parts_to_remove)
	{
		LOG_DEBUG(log, "Removing part " << part->name);
		part->remove();
	}
}

void MergeTreeData::setPath(const String & new_full_path, bool move_data)
{
	if (move_data)
	{
		if (Poco::File{new_full_path}.exists())
			throw Exception{
				"Target path already exists: " + new_full_path,
				/// @todo existing target can also be a file, not directory
				ErrorCodes::DIRECTORY_ALREADY_EXISTS
			};
		Poco::File(full_path).renameTo(new_full_path);
		/// Если данные перемещать не нужно, значит их переместил кто-то другой. Расчитываем, что он еще и сбросил кеши.
		context.resetCaches();
	}

	full_path = new_full_path;
}

void MergeTreeData::dropAllData()
{
	data_parts.clear();
	all_data_parts.clear();
	column_sizes.clear();

	context.resetCaches();

	Poco::File(full_path).remove(true);
}


void MergeTreeData::checkAlter(const AlterCommands & params)
{
	/// Проверим, что указанные преобразования можно совершить над списком столбцов без учета типов.
	auto new_columns = *columns;
	auto new_materialized_columns = materialized_columns;
	auto new_alias_columns = alias_columns;
	auto new_column_defaults = column_defaults;
	params.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

	/// Список столбцов, которые нельзя трогать.
	/// sampling_expression можно не учитывать, потому что он обязан содержаться в первичном ключе.

	Names keys;

	if (primary_expr)
		keys = primary_expr->getRequiredColumns();

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

	/// augment plain columns with materialized columns for convert expression creation
	new_columns.insert(std::end(new_columns),
		std::begin(new_materialized_columns), std::end(new_materialized_columns));
	createConvertExpression(nullptr, getColumnsList(), new_columns, unused_expression, unused_map);
}

void MergeTreeData::createConvertExpression(const DataPartPtr & part, const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns,
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
			const auto new_type = new_types[column.name].get();
			const String new_type_name = new_type->getName();

			if (new_type_name != column.type->getName() &&
				(!part || part->hasColumnFiles(column.name)))
			{
				/// Нужно изменить тип столбца.

				if (!out_expression)
					out_expression = new ExpressionActions(NamesAndTypesList(), context.getSettingsRef());

				out_expression->addInput(ColumnWithTypeAndName(nullptr, column.type, column.name));

				Names out_names;

				if (const auto fixed_string = typeid_cast<const DataTypeFixedString *>(new_type))
				{
					const auto width = fixed_string->getN();
					const auto string_width_column = toString(width);
					out_expression->addInput({ new ColumnConstUInt64{1, width}, new DataTypeUInt64, string_width_column });

					const auto function = FunctionFactory::instance().get("toFixedString", context);
					out_expression->add(ExpressionAction::applyFunction(function, Names{
						column.name, string_width_column
					}), out_names);

					out_expression->add(ExpressionAction::removeColumn(string_width_column));
				}
				else
				{
					const FunctionPtr & function = FunctionFactory::instance().get("to" + new_type_name, context);
					out_expression->add(ExpressionAction::applyFunction(function, Names{column.name}), out_names);
				}

				out_expression->add(ExpressionAction::removeColumn(column.name));

				const String escaped_expr = escapeForFileName(out_names[0]);
				const String escaped_column = escapeForFileName(column.name);
				out_rename_map[escaped_expr + ".bin"] = escaped_column + ".bin";
				out_rename_map[escaped_expr + ".mrk"] = escaped_column + ".mrk";
			}
		}
	}
}

MergeTreeData::AlterDataPartTransactionPtr MergeTreeData::alterDataPart(
	const DataPartPtr & part, const NamesAndTypesList & new_columns, bool skip_sanity_checks)
{
	ExpressionActionsPtr expression;
	AlterDataPartTransactionPtr transaction(new AlterDataPartTransaction(part)); /// Блокирует изменение куска.
	createConvertExpression(part, part->columns, new_columns, expression, transaction->rename_map);

	if (!skip_sanity_checks && transaction->rename_map.size() > 5)
	{
		transaction->clear();

		throw Exception("Suspiciously many (" + toString(transaction->rename_map.size()) + ") files need to be modified in part " + part->name
						+ ". Aborting just in case");
	}

	if (transaction->rename_map.empty())
	{
		transaction->clear();
		return nullptr;
	}

	DataPart::Checksums add_checksums;

	/// Применим выражение и запишем результат во временные файлы.
	if (expression)
	{
		MarkRanges ranges(1, MarkRange(0, part->size));
		BlockInputStreamPtr part_in = new MergeTreeBlockInputStream(full_path + part->name + '/',
			DEFAULT_MERGE_BLOCK_SIZE, expression->getRequiredColumns(), *this, part, ranges, false, nullptr, "", false, 0, DBMS_DEFAULT_BUFFER_SIZE);
		ExpressionBlockInputStream in(part_in, expression);
		MergedColumnOnlyOutputStream out(*this, full_path + part->name + '/', true, CompressionMethod::LZ4);
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
		new_checksums.write(checksums_file);
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

		/// TODO: можно не сбрасывать кеши при добавлении столбца.
		data_part->storage.context.resetCaches();

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
					Poco::File file(path + it.first);
					if (file.exists())
						file.remove();
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


void MergeTreeData::renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction)
{
	auto removed = renameTempPartAndReplace(part, increment, out_transaction);
	if (!removed.empty())
	{
		LOG_ERROR(log, "Added part " << part->name << + " covers " << toString(removed.size())
			<< " existing part(s) (including " << removed[0]->name << ")");
	}
}

MergeTreeData::DataPartsVector MergeTreeData::renameTempPartAndReplace(
	MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction)
{
	if (out_transaction && out_transaction->data)
		throw Exception("Using the same MergeTreeData::Transaction for overlapping transactions is invalid", ErrorCodes::LOGICAL_ERROR);

	LOG_TRACE(log, "Renaming " << part->name << ".");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	String old_name = part->name;
	String old_path = getFullPath() + old_name + "/";

	/** Для StorageMergeTree важно, что получение номера куска происходит атомарно с добавлением этого куска в набор.
	  * Иначе есть race condition - может произойти слияние пары кусков, диапазоны номеров которых
	  *  содержат ещё не добавленный кусок.
	  */
	if (increment)
		part->left = part->right = increment->get();

	String new_name = ActiveDataPartSet::getPartName(part->left_date, part->right_date, part->left, part->right, part->level);

	part->is_temp = false;
	part->name = new_name;
	bool duplicate = data_parts.count(part);
	part->name = old_name;
	part->is_temp = true;

	if (duplicate)
		throw Exception("Part " + new_name + " already exists", ErrorCodes::DUPLICATE_DATA_PART);

	String new_path = getFullPath() + new_name + "/";

	/// Переименовываем кусок.
	Poco::File(old_path).renameTo(new_path);

	part->is_temp = false;
	part->name = new_name;

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
		removePartContributionToColumnSizes(*it);
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
		removePartContributionToColumnSizes(*it);
		data_parts.erase(it++);
	}

	if (obsolete)
	{
		LOG_WARNING(log, "Obsolete part " + part->name + " added");
		part->remove_time = time(0);
	}
	else
	{
		data_parts.insert(part);
		addPartContributionToColumnSizes(part);
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
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	for (const DataPartPtr & part : remove)
	{
		part->remove_time = clear_without_timeout ? 0 : time(0);
		removePartContributionToColumnSizes(part);
		data_parts.erase(part);
	}
	for (const DataPartPtr & part : add)
	{
		data_parts.insert(part);
		addPartContributionToColumnSizes(part);
	}
}

void MergeTreeData::attachPart(const DataPartPtr & part)
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	if (!all_data_parts.insert(part).second)
		throw Exception("Part " + part->name + " is already attached", ErrorCodes::DUPLICATE_DATA_PART);

	data_parts.insert(part);
	addPartContributionToColumnSizes(part);
}

void MergeTreeData::renameAndDetachPart(const DataPartPtr & part, const String & prefix, bool restore_covered, bool move_to_detached)
{
	LOG_INFO(log, "Renaming " << part->name << " to " << prefix << part->name << " and detaching it.");

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);
	Poco::ScopedLock<Poco::FastMutex> lock_all(all_data_parts_mutex);

	if (!all_data_parts.erase(part))
		throw Exception("No such data part", ErrorCodes::NO_SUCH_DATA_PART);

	removePartContributionToColumnSizes(part);
	data_parts.erase(part);
	if (move_to_detached || !prefix.empty())
		part->renameAddPrefix(move_to_detached, prefix);

	if (restore_covered)
	{
		auto it = all_data_parts.lower_bound(part);
		Strings restored;
		bool error = false;

		Int64 pos = part->left;

		if (it != all_data_parts.begin())
		{
			--it;
			if (part->contains(**it))
			{
				if ((*it)->left != part->left)
					error = true;
				data_parts.insert(*it);
				addPartContributionToColumnSizes(*it);
				pos = (*it)->right + 1;
				restored.push_back((*it)->name);
			}
			else
				error = true;
			++it;
		}
		else
			error = true;

		for (; it != all_data_parts.end() && part->contains(**it); ++it)
		{
			if ((*it)->left < pos)
				continue;
			if ((*it)->left > pos)
				error = true;
			data_parts.insert(*it);
			addPartContributionToColumnSizes(*it);
			pos = (*it)->right + 1;
			restored.push_back((*it)->name);
		}

		if (pos != part->right + 1)
			error = true;

		for (const String & name : restored)
		{
			LOG_INFO(log, "Activated part " << name);
		}

		if (error)
			LOG_ERROR(log, "The set of parts restored in place of " << part->name << " looks incomplete. There might or might not be a data loss.");
	}
}

void MergeTreeData::detachPartInPlace(const DataPartPtr & part)
{
	renameAndDetachPart(part, "", false, false);
}

MergeTreeData::DataParts MergeTreeData::getDataParts()
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	return data_parts;
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVector()
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	return DataPartsVector(std::begin(data_parts), std::end(data_parts));
}

size_t MergeTreeData::getTotalActiveSizeInBytes()
{
	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

	size_t res = 0;
	for (auto & part : data_parts)
		res += part->size_in_bytes;

	return res;
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
		if (part->month == cur_month)
		{
			++cur_count;
		}
		else
		{
			cur_month = part->month;
			cur_count = 1;
		}

		res = std::max(res, cur_count);
	}

	return res;
}

void MergeTreeData::delayInsertIfNeeded(Poco::Event * until)
{
	size_t parts_count = getMaxPartsCountForMonth();
	if (parts_count > settings.parts_to_delay_insert)
	{
		double delay = std::pow(settings.insert_delay_step, parts_count - settings.parts_to_delay_insert);
		delay /= 1000;

		if (delay > DBMS_MAX_DELAY_OF_INSERT)
		{
			ProfileEvents::increment(ProfileEvents::RejectedInserts);
			throw Exception("Too much parts. Merges are processing significantly slower than inserts.", ErrorCodes::TOO_MUCH_PARTS);
		}

		ProfileEvents::increment(ProfileEvents::DelayedInserts);
		ProfileEvents::increment(ProfileEvents::DelayedInsertsMilliseconds, delay * 1000);

		LOG_INFO(log, "Delaying inserting block by "
			<< std::fixed << std::setprecision(4) << delay << " sec. because there are " << parts_count << " parts");

		if (until)
			until->tryWait(delay * 1000);
		else
			std::this_thread::sleep_for(std::chrono::duration<double>(delay));
	}
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const String & part_name)
{
	MutableDataPartPtr tmp_part(new DataPart(*this));
	ActiveDataPartSet::parsePartName(part_name, *tmp_part);

	Poco::ScopedLock<Poco::FastMutex> lock(data_parts_mutex);

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

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const String & part_name)
{
	MutableDataPartPtr tmp_part(new DataPart(*this));
	ActiveDataPartSet::parsePartName(part_name, *tmp_part);

	Poco::ScopedLock<Poco::FastMutex> lock(all_data_parts_mutex);
	DataParts::iterator it = all_data_parts.lower_bound(tmp_part);
	if (it != all_data_parts.end() && (*it)->name == part_name)
		return *it;

	return nullptr;
}

MergeTreeData::MutableDataPartPtr MergeTreeData::loadPartAndFixMetadata(const String & relative_path)
{
	MutableDataPartPtr part = std::make_shared<DataPart>(*this);
	part->name = relative_path;
	ActiveDataPartSet::parsePartName(Poco::Path(relative_path).getFileName(), *part);

	/// Раньше список столбцов записывался неправильно. Удалим его и создадим заново.
	if (Poco::File(full_path + relative_path + "/columns.txt").exists())
		Poco::File(full_path + relative_path + "/columns.txt").remove();

	part->loadColumns(false);
	part->loadChecksums(false);
	part->loadIndex();
	part->checkNotBroken(false);

	part->modification_time = Poco::File(full_path + relative_path).getLastModified().epochTime();

	/// Если нет файла с чексуммами, посчитаем чексуммы и запишем. Заодно проверим данные.
	if (part->checksums.empty())
	{
		MergeTreePartChecker::Settings settings;
		settings.setIndexGranularity(index_granularity);
		settings.setRequireColumnFiles(true);
		MergeTreePartChecker::checkDataPart(full_path + relative_path, settings, primary_key_sample, &part->checksums);

		{
			WriteBufferFromFile out(full_path + relative_path + "/checksums.txt.tmp", 4096);
			part->checksums.write(out);
		}

		Poco::File(full_path + relative_path + "/checksums.txt.tmp").renameTo(full_path + relative_path + "/checksums.txt");
	}

	return part;
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

bool MergeTreeData::DataPart::Checksums::read(ReadBuffer & in)
{
	files.clear();

	DB::assertString("checksums format version: ", in);
	int format_version;
	DB::readText(format_version, in);
	DB::assertString("\n", in);

	if (format_version < 1 || format_version > 4)
		throw Exception("Bad checksums format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT);

	if (format_version == 1)
		return false;
	if (format_version == 2)
		return read_v2(in);
	if (format_version == 3)
		return read_v3(in);
	if (format_version == 4)
		return read_v4(in);

	return false;
}

bool MergeTreeData::DataPart::Checksums::read_v2(ReadBuffer & in)
{
	size_t count;

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

bool MergeTreeData::DataPart::Checksums::read_v3(ReadBuffer & in)
{
	size_t count;

	DB::readVarUInt(count, in);

	for (size_t i = 0; i < count; ++i)
	{
		String name;
		Checksum sum;

		DB::readBinary(name, in);
		DB::readVarUInt(sum.file_size, in);
		DB::readBinary(sum.file_hash, in);
		DB::readBinary(sum.is_compressed, in);

		if (sum.is_compressed)
		{
			DB::readVarUInt(sum.uncompressed_size, in);
			DB::readBinary(sum.uncompressed_hash, in);
		}

		files.emplace(std::move(name), sum);
	}

	return true;
}

bool MergeTreeData::DataPart::Checksums::read_v4(ReadBuffer & from)
{
	CompressedReadBuffer in{from};
	return read_v3(in);
}

void MergeTreeData::DataPart::Checksums::write(WriteBuffer & to) const
{
	DB::writeString("checksums format version: 4\n", to);

	DB::CompressedWriteBuffer out{to, CompressionMethod::LZ4, 1 << 16};
	DB::writeVarUInt(files.size(), out);

	for (const auto & it : files)
	{
		const String & name = it.first;
		const Checksum & sum = it.second;

		DB::writeBinary(name, out);
		DB::writeVarUInt(sum.file_size, out);
		DB::writeBinary(sum.file_hash, out);
		DB::writeBinary(sum.is_compressed, out);

		if (sum.is_compressed)
		{
			DB::writeVarUInt(sum.uncompressed_size, out);
			DB::writeBinary(sum.uncompressed_hash, out);
		}
	}
}

void MergeTreeData::calculateColumnSizes()
{
	column_sizes.clear();

	for (const auto & part : data_parts)
		addPartContributionToColumnSizes(part);
}

void MergeTreeData::addPartContributionToColumnSizes(const DataPartPtr & part)
{
	const auto & files = part->checksums.files;

	for (const auto & column : *columns)
	{
		const auto escaped_name = escapeForFileName(column.name);
		const auto bin_file_name = escaped_name + ".bin";
		const auto mrk_file_name = escaped_name + ".mrk";

		auto & column_size = column_sizes[column.name];

		if (files.count(bin_file_name)) column_size += files.find(bin_file_name)->second.file_size;
		if (files.count(mrk_file_name)) column_size += files.find(mrk_file_name)->second.file_size;
	}
}

void MergeTreeData::removePartContributionToColumnSizes(const DataPartPtr & part)
{
	const auto & files = part->checksums.files;

	for (const auto & column : *columns)
	{
		const auto escaped_name = escapeForFileName(column.name);
		const auto bin_file_name = escaped_name + ".bin";
		const auto mrk_file_name = escaped_name + ".mrk";

		auto & column_size = column_sizes[column.name];

		if (files.count(bin_file_name)) column_size -= files.find(bin_file_name)->second.file_size;
		if (files.count(mrk_file_name)) column_size -= files.find(mrk_file_name)->second.file_size;
	}
}


void MergeTreeData::freezePartition(const std::string & prefix)
{
	LOG_DEBUG(log, "Freezing parts with prefix " + prefix);

	String clickhouse_path = Poco::Path(context.getPath()).makeAbsolute().toString();
	String shadow_path = clickhouse_path + "shadow/";
	Poco::File(shadow_path).createDirectories();
	String backup_path = shadow_path + toString(Increment(shadow_path + "increment.txt").get(true)) + "/";

	LOG_DEBUG(log, "Snapshot will be placed at " + backup_path);

	size_t parts_processed = 0;
	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(full_path); it != end; ++it)
	{
		if (0 == it.name().compare(0, prefix.size(), prefix))
		{
			LOG_DEBUG(log, "Freezing part " + it.name());

			String part_absolute_path = it.path().absolute().toString();
			if (0 != part_absolute_path.compare(0, clickhouse_path.size(), clickhouse_path))
				throw Exception("Part path " + part_absolute_path + " is not inside " + clickhouse_path, ErrorCodes::LOGICAL_ERROR);

			String backup_part_absolute_path = part_absolute_path;
			backup_part_absolute_path.replace(0, clickhouse_path.size(), backup_path);
			localBackup(part_absolute_path, backup_part_absolute_path);
			++parts_processed;
		}
	}

	LOG_DEBUG(log, "Freezed " << parts_processed << " parts");
}


static std::pair<String, DayNum_t> getMonthNameAndDayNum(const Field & partition)
{
	String month_name = partition.getType() == Field::Types::UInt64
		? toString(partition.get<UInt64>())
		: partition.safeGet<String>();

	if (month_name.size() != 6 || !std::all_of(month_name.begin(), month_name.end(), isdigit))
		throw Exception("Invalid partition format: " + month_name + ". Partition should consist of 6 digits: YYYYMM",
			ErrorCodes::INVALID_PARTITION_NAME);

	DayNum_t date = DateLUT::instance().YYYYMMDDToDayNum(parse<UInt32>(month_name + "01"));

	/// Не можем просто сравнить date с нулем, потому что 0 тоже валидный DayNum.
	if (month_name != toString(DateLUT::instance().toNumYYYYMMDD(date) / 100))
		throw Exception("Invalid partition format: " + month_name + " doesn't look like month.",
			ErrorCodes::INVALID_PARTITION_NAME);

	return std::make_pair(month_name, date);
}


String MergeTreeData::getMonthName(const Field & partition)
{
	return getMonthNameAndDayNum(partition).first;
}

DayNum_t MergeTreeData::getMonthDayNum(const Field & partition)
{
	return getMonthNameAndDayNum(partition).second;
}

}
