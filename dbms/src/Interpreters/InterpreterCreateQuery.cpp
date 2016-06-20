#include <memory>

#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DB/DataStreams/ProhibitColumnsBlockOutputStream.h>
#include <DB/DataStreams/MaterializingBlockOutputStream.h>
#include <DB/DataStreams/AddingDefaultBlockOutputStream.h>
#include <DB/DataStreams/PushingToViewsBlockOutputStream.h>

#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTColumnDeclaration.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Storages/StorageFactory.h>
#include <DB/Storages/StorageLog.h>
#include <DB/Storages/System/StorageSystemNumbers.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeFactory.h>

#include <DB/Databases/DatabaseFactory.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int DIRECTORY_DOESNT_EXIST;
	extern const int DIRECTORY_ALREADY_EXISTS;
	extern const int TABLE_ALREADY_EXISTS;
	extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
	extern const int INCORRECT_QUERY;
	extern const int ENGINE_REQUIRED;
	extern const int TABLE_METADATA_ALREADY_EXISTS;
	extern const int UNKNOWN_DATABASE_ENGINE;
}


InterpreterCreateQuery::InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}


void InterpreterCreateQuery::createDatabase(ASTCreateQuery & create)
{
	String database_name = create.database;

	if (create.if_not_exists && context.isDatabaseExist(database_name))
		return;

	String database_engine_name;
	if (!create.storage)
	{
		database_engine_name = "Ordinary";	/// Движок баз данных по-умолчанию.
		auto func = std::make_shared<ASTFunction>();
		func->name = database_engine_name;
		create.storage = func;
	}
	else
	{
		const ASTFunction & engine_id = typeid_cast<const ASTFunction &>(*create.storage);

		/// На данный момент, движков таблиц с аргументами не бывает.
		if (engine_id.arguments || engine_id.parameters)
		{
			std::stringstream ostr;
			formatAST(*create.storage, ostr, 0, false, false);
			throw Exception("Unknown database engine: " + ostr.str(), ErrorCodes::UNKNOWN_DATABASE_ENGINE);
		}

		database_engine_name = engine_id.name;
	}

	String database_name_escaped = escapeForFileName(database_name);

	/// Создаём директории с данными и метаданными таблиц.
	String path = context.getPath();
	String data_path = path + "data/" + database_name_escaped + "/";
	String metadata_path = path + "metadata/" + database_name_escaped + "/";

	Poco::File(metadata_path).createDirectory();
	Poco::File(data_path).createDirectory();

	DatabasePtr database = DatabaseFactory::get(database_engine_name, database_name, metadata_path, context);

	/// Записываем файл с метаданными, если нужно.
	String metadata_file_tmp_path = path + "metadata/" + database_name_escaped + ".sql.tmp";
	String metadata_file_path = path + "metadata/" + database_name_escaped + ".sql";

	bool need_write_metadata = !create.attach;

	if (need_write_metadata)
	{
		create.attach = true;
		create.if_not_exists = false;

		std::ostringstream statement_stream;
		formatAST(create, statement_stream, 0, false);
		statement_stream << '\n';
		String statement = statement_stream.str();

		/// Гарантирует, что база данных не создаётся прямо сейчас.
		WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
		writeString(statement, out);
		out.next();
		out.sync();
		out.close();
	}

	try
	{
		context.addDatabase(database_name, database);

		if (need_write_metadata)
			Poco::File(metadata_file_tmp_path).renameTo(metadata_file_path);

		database->loadTables(context, thread_pool);
	}
	catch (...)
	{
		if (need_write_metadata)
			Poco::File(metadata_file_tmp_path).remove();

		throw;
	}
}


using ColumnsAndDefaults = std::pair<NamesAndTypesList, ColumnDefaults>;

/// AST в список столбцов с типами. Столбцы типа Nested развернуты в список настоящих столбцов.
static ColumnsAndDefaults parseColumns(
	ASTPtr expression_list, const Context & context)
{
	auto & column_list_ast = typeid_cast<ASTExpressionList &>(*expression_list);

	/// list of table columns in correct order
	NamesAndTypesList columns{};
	ColumnDefaults defaults{};

	/// Columns requiring type-deduction or default_expression type-check
	std::vector<std::pair<NameAndTypePair *, ASTColumnDeclaration *>> defaulted_columns{};

	/** all default_expressions as a single expression list,
	 *  mixed with conversion-columns for each explicitly specified type */
	ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
	default_expr_list->children.reserve(column_list_ast.children.size());

	const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

	for (auto & ast : column_list_ast.children)
	{
		auto & col_decl = typeid_cast<ASTColumnDeclaration &>(*ast);

		if (col_decl.type)
		{
			const auto & type_range = col_decl.type->range;
			columns.emplace_back(col_decl.name,
				data_type_factory.get({ type_range.first, type_range.second }));
		}
		else
			/// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
			columns.emplace_back(col_decl.name, std::make_shared<DataTypeUInt8>());

		/// add column to postprocessing if there is a default_expression specified
		if (col_decl.default_expression)
		{
			defaulted_columns.emplace_back(&columns.back(), &col_decl);

			/** for columns with explicitly-specified type create two expressions:
			 *	1. default_expression aliased as column name with _tmp suffix
			 *	2. conversion of expression (1) to explicitly-specified type alias as column name */
			if (col_decl.type)
			{
				const auto & final_column_name = col_decl.name;
				const auto tmp_column_name = final_column_name + "_tmp";
				const auto data_type_ptr = columns.back().type.get();

				default_expr_list->children.emplace_back(setAlias(
					makeASTFunction("CAST", std::make_shared<ASTIdentifier>(StringRange(), tmp_column_name),
						std::make_shared<ASTLiteral>(StringRange(), Field(data_type_ptr->getName()))), final_column_name));
				default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), tmp_column_name));
			}
			else
				default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
		}
	}

	/// set missing types and wrap default_expression's in a conversion-function if necessary
	if (!defaulted_columns.empty())
	{
		const auto actions = ExpressionAnalyzer{default_expr_list, context, {}, columns}.getActions(true);
		const auto block = actions->getSampleBlock();

		for (auto & column : defaulted_columns)
		{
			const auto name_and_type_ptr = column.first;
			const auto col_decl_ptr = column.second;

			const auto & column_name = col_decl_ptr->name;
			const auto has_explicit_type = nullptr != col_decl_ptr->type;
			auto & explicit_type = name_and_type_ptr->type;

			/// if column declaration contains explicit type, name_and_type_ptr->type is not null
			if (has_explicit_type)
			{
				const auto & tmp_column = block.getByName(column_name + "_tmp");
				const auto & deduced_type = tmp_column.type;

				/// type mismatch between explicitly specified and deduced type, add conversion for non-array types
				if (explicit_type->getName() != deduced_type->getName())
				{
					col_decl_ptr->default_expression = makeASTFunction("CAST", col_decl_ptr->default_expression,
						std::make_shared<ASTLiteral>(StringRange(), explicit_type->getName()));

					col_decl_ptr->children.clear();
					col_decl_ptr->children.push_back(col_decl_ptr->type);
					col_decl_ptr->children.push_back(col_decl_ptr->default_expression);
				}
			}
			else
				/// no explicit type, name_and_type_ptr->type is null, set to deduced type
				explicit_type = block.getByName(column_name).type;

			defaults.emplace(column_name, ColumnDefault{
				columnDefaultTypeFromString(col_decl_ptr->default_specifier),
				col_decl_ptr->default_expression
			});
		}
	}

	return { *DataTypeNested::expandNestedColumns(columns), defaults };
}


static NamesAndTypesList removeAndReturnColumns(
	ColumnsAndDefaults & columns_and_defaults, const ColumnDefaultType type)
{
	auto & columns = columns_and_defaults.first;
	auto & defaults = columns_and_defaults.second;

	NamesAndTypesList removed{};

	for (auto it = std::begin(columns); it != std::end(columns);)
	{
		const auto jt = defaults.find(it->name);
		if (jt != std::end(defaults) && jt->second.type == type)
		{
			removed.push_back(*it);
			it = columns.erase(it);
		}
		else
			++it;
	}

	return removed;
}


ASTPtr InterpreterCreateQuery::formatColumns(const NamesAndTypesList & columns)
{
	auto columns_list = std::make_shared<ASTExpressionList>();

	for (const auto & column : columns)
	{
		const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
		column_declaration->name = column.name;

		StringPtr type_name = std::make_shared<String>(column.type->getName());
		auto pos = type_name->data();
		const auto end = pos + type_name->size();

		ParserIdentifierWithOptionalParameters storage_p;
		column_declaration->type = parseQuery(storage_p, pos, end, "data type");
		column_declaration->type->query_string = type_name;
		columns_list->children.emplace_back(column_declaration);
	}

	return columns_list;
}

ASTPtr InterpreterCreateQuery::formatColumns(NamesAndTypesList columns,
	const NamesAndTypesList & materialized_columns,
	const NamesAndTypesList & alias_columns,
	const ColumnDefaults & column_defaults)
{
	columns.insert(std::end(columns), std::begin(materialized_columns), std::end(materialized_columns));
	columns.insert(std::end(columns), std::begin(alias_columns), std::end(alias_columns));

	auto columns_list = std::make_shared<ASTExpressionList>();

	for (const auto & column : columns)
	{
		const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
		ASTPtr column_declaration_ptr{column_declaration};

		column_declaration->name = column.name;

		StringPtr type_name = std::make_shared<String>(column.type->getName());
		auto pos = type_name->data();
		const auto end = pos + type_name->size();

		ParserIdentifierWithOptionalParameters storage_p;
		column_declaration->type = parseQuery(storage_p, pos, end, "data type");
		column_declaration->type->query_string = type_name;

		const auto it = column_defaults.find(column.name);
		if (it != std::end(column_defaults))
		{
			column_declaration->default_specifier = toString(it->second.type);
			column_declaration->default_expression = it->second.expression->clone();
		}

		columns_list->children.push_back(column_declaration_ptr);
	}

	return columns_list;
}


InterpreterCreateQuery::ColumnsInfo InterpreterCreateQuery::getColumnsInfo(
	const ASTPtr & columns, const Context & context)
{
	ColumnsInfo res;

	auto && columns_and_defaults = parseColumns(columns, context);
	res.materialized_columns = removeAndReturnColumns(columns_and_defaults, ColumnDefaultType::Materialized);
	res.alias_columns = removeAndReturnColumns(columns_and_defaults, ColumnDefaultType::Alias);
	res.columns = std::make_shared<NamesAndTypesList>(std::move(columns_and_defaults.first));
	res.column_defaults = std::move(columns_and_defaults.second);

	if (res.columns->size() + res.materialized_columns.size() == 0)
		throw Exception{"Cannot CREATE table without physical columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED};

	return res;
}


InterpreterCreateQuery::ColumnsInfo InterpreterCreateQuery::setColumns(
	ASTCreateQuery & create, const Block & as_select_sample, const StoragePtr & as_storage) const
{
	ColumnsInfo res;

	if (create.columns)
	{
		res = getColumnsInfo(create.columns, context);
	}
	else if (!create.as_table.empty())
	{
		res.columns = std::make_shared<NamesAndTypesList>(as_storage->getColumnsListNonMaterialized());
		res.materialized_columns = as_storage->materialized_columns;
		res.alias_columns = as_storage->alias_columns;
		res.column_defaults = as_storage->column_defaults;
	}
	else if (create.select)
	{
		res.columns = std::make_shared<NamesAndTypesList>();
		for (size_t i = 0; i < as_select_sample.columns(); ++i)
			res.columns->push_back(NameAndTypePair(as_select_sample.getByPosition(i).name, as_select_sample.getByPosition(i).type));
	}
	else
		throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

	/// Даже если в запросе был список столбцов, на всякий случай приведем его к стандартному виду (развернём Nested).
	ASTPtr new_columns = formatColumns(*res.columns, res.materialized_columns, res.alias_columns, res.column_defaults);
	if (create.columns)
	{
		auto it = std::find(create.children.begin(), create.children.end(), create.columns);
		if (it != create.children.end())
			*it = new_columns;
		else
			create.children.push_back(new_columns);
	}
	else
		create.children.push_back(new_columns);
	create.columns = new_columns;

	return res;
}


String InterpreterCreateQuery::setEngine(
	ASTCreateQuery & create, const StoragePtr & as_storage) const
{
	String storage_name;

	auto set_engine = [&](const char * engine)
	{
		storage_name = engine;
		auto func = std::make_shared<ASTFunction>();
		func->name = engine;
		create.storage = func;
	};

	if (create.storage)
	{
		storage_name = typeid_cast<ASTFunction &>(*create.storage).name;
	}
	else if (!create.as_table.empty())
	{
		/// NOTE Получение структуры у таблицы, указанной в AS делается не атомарно с созданием таблицы.

		String as_database_name = create.as_database.empty() ? context.getCurrentDatabase() : create.as_database;
		String as_table_name = create.as_table;

		storage_name = as_storage->getName();
		create.storage = typeid_cast<const ASTCreateQuery &>(*context.getCreateQuery(as_database_name, as_table_name)).storage;
	}
	else if (create.is_temporary)
		set_engine("Memory");
	else if (create.is_view)
		set_engine("View");
	else if (create.is_materialized_view)
		set_engine("MaterializedView");
	else
		throw Exception("Incorrect CREATE query: required ENGINE.", ErrorCodes::ENGINE_REQUIRED);

	return storage_name;
}


BlockIO InterpreterCreateQuery::createTable(ASTCreateQuery & create)
{
	String path = context.getPath();
	String current_database = context.getCurrentDatabase();

	String database_name = create.database.empty() ? current_database : create.database;
	String database_name_escaped = escapeForFileName(database_name);
	String table_name = create.table;
	String table_name_escaped = escapeForFileName(table_name);

	String data_path = path + "data/" + database_name_escaped + "/";
	String metadata_path = path + "metadata/" + database_name_escaped + "/" + table_name_escaped + ".sql";

	std::unique_ptr<InterpreterSelectQuery> interpreter_select;
	Block as_select_sample;
	/// Для таблиц типа view, чтобы получить столбцы, может понадобиться sample_block.
	if (create.select && (!create.attach || (!create.columns && (create.is_view || create.is_materialized_view))))
	{
		interpreter_select = std::make_unique<InterpreterSelectQuery>(create.select, context);
		as_select_sample = interpreter_select->getSampleBlock();
	}

	String as_database_name = create.as_database.empty() ? current_database : create.as_database;
	String as_table_name = create.as_table;

	StoragePtr as_storage;
	IStorage::TableStructureReadLockPtr as_storage_lock;
	if (!as_table_name.empty())
	{
		as_storage = context.getTable(as_database_name, as_table_name);
		as_storage_lock = as_storage->lockStructure(false);
	}

	/// Устанавливаем и получаем список столбцов.
	ColumnsInfo columns = setColumns(create, as_select_sample, as_storage);

	/// Выбор нужного движка таблицы
	String storage_name = setEngine(create, as_storage);

	StoragePtr res;

	{
		std::unique_ptr<DDLGuard> guard;

		if (!create.is_temporary)
		{
			context.assertDatabaseExists(database_name);

			/** Если таблица уже существует, и в запросе указано IF NOT EXISTS,
			  *  то мы разрешаем конкуррентные запросы CREATE (которые ничего не делают).
			  * Иначе конкуррентные запросы на создание таблицы, если таблицы не существует,
			  *  могут кидать исключение, даже если указано IF NOT EXISTS.
			  */
			guard = context.getDDLGuardIfTableDoesntExist(database_name, table_name,
				"Table " + database_name + "." + table_name + " is creating or attaching right now");

			if (!guard)
			{
				if (create.if_not_exists)
					return {};
				else
					throw Exception("Table " + database_name + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
			}
		}

		res = StorageFactory::instance().get(
			storage_name, data_path, table_name, database_name, context,
			context.getGlobalContext(), query_ptr, columns.columns,
			columns.materialized_columns, columns.alias_columns, columns.column_defaults, create.attach);

		if (create.is_temporary)
			context.getSessionContext().addExternalTable(table_name, res);
		else
			context.getDatabase(database_name)->createTable(table_name, res, query_ptr, storage_name);
	}

	/// Если запрос CREATE SELECT, то вставим в таблицу данные
	if (create.select && storage_name != "View" && (storage_name != "MaterializedView" || create.is_populate))
	{
		auto table_lock = res->lockStructure(true);

		/// Также см. InterpreterInsertQuery.
		BlockOutputStreamPtr out =
			std::make_shared<ProhibitColumnsBlockOutputStream>(
				std::make_shared<AddingDefaultBlockOutputStream>(
					std::make_shared<MaterializingBlockOutputStream>(
						std::make_shared<PushingToViewsBlockOutputStream>(
							create.database, create.table,
							create.is_temporary ? context.getSessionContext() : context,
							query_ptr)),
					/// @note shouldn't these two contexts be session contexts in case of temporary table?
					columns.columns, columns.column_defaults, context, static_cast<bool>(context.getSettingsRef().strict_insert_defaults)),
				columns.materialized_columns);

		BlockIO io;
		io.in_sample = as_select_sample;
		io.in = std::make_shared<NullAndDoCopyBlockInputStream>(interpreter_select->execute().in, out);

		return io;
	}

	return {};
}


BlockIO InterpreterCreateQuery::execute()
{
	ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_ptr);

	/// CREATE|ATTACH DATABASE
	if (!create.database.empty() && create.table.empty())
	{
		createDatabase(create);
		return {};
	}
	else
		return createTable(create);
}


}
