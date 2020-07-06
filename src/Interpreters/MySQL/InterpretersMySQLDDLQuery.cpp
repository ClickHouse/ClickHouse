#include <Interpreters/MySQL/InterpretersMySQLDDLQuery.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTCreateDefines.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int NOT_IMPLEMENTED;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}

namespace MySQLInterpreter
{

static inline NamesAndTypesList getColumnsList(ASTExpressionList * columns_define)
{
    NamesAndTypesList columns_name_and_type;
    for (size_t index = 0; index < columns_define->children.size(); ++index)
    {
        const auto & declare_column = columns_define->children[index]->as<MySQLParser::ASTDeclareColumn>();

        if (!declare_column || !declare_column->data_type)
            throw Exception("Missing type in definition of column.", ErrorCodes::UNKNOWN_TYPE);

        bool is_nullable = true;
        if (declare_column->column_options)
        {
            if (const auto * options = declare_column->column_options->as<MySQLParser::ASTDeclareOptions>();
                options && options->changes.count("is_null"))
                is_nullable = options->changes.at("is_null")->as<ASTLiteral>()->value.safeGet<UInt64>();
        }

        ASTPtr data_type = declare_column->data_type;

        if (is_nullable)
            data_type = makeASTFunction("Nullable", data_type);

        columns_name_and_type.emplace_back(declare_column->name, DataTypeFactory::instance().get(data_type));
    }

    return columns_name_and_type;
}

static NamesAndTypesList getNames(const ASTFunction & expr, const Context & context, const NamesAndTypesList & columns)
{
    if (expr.arguments->children.empty())
        return NamesAndTypesList{};

    ASTPtr temp_ast = expr.clone();
    auto syntax = SyntaxAnalyzer(context).analyze(temp_ast, columns);
    auto expression = ExpressionAnalyzer(temp_ast, syntax, context).getActions(true);
    return expression->getRequiredColumnsWithTypes();
}

static inline std::tuple<NamesAndTypesList, NamesAndTypesList, NamesAndTypesList, NameSet> getKeys(
    ASTExpressionList * columns_define, ASTExpressionList * indices_define, const Context & context, const NamesAndTypesList & columns)
{
    NameSet increment_columns;
    auto keys = makeASTFunction("tuple");
    auto unique_keys = makeASTFunction("tuple");
    auto primary_keys = makeASTFunction("tuple");

    if (indices_define && !indices_define->children.empty())
    {
        for (size_t index = 0; index < indices_define->children.size(); ++index)
        {
            const auto & declare_index = indices_define->children[index]->as<MySQLParser::ASTDeclareIndex>();

            /// flatten
            if (startsWith(declare_index->index_type, "KEY_"))
                keys->arguments->children.insert(keys->arguments->children.end(),
                    declare_index->index_columns->children.begin(), declare_index->index_columns->children.end());
            else if (startsWith(declare_index->index_type, "UNIQUE_"))
                unique_keys->arguments->children.insert(keys->arguments->children.end(),
                    declare_index->index_columns->children.begin(), declare_index->index_columns->children.end());
            if (startsWith(declare_index->index_type, "PRIMARY_KEY_"))
                primary_keys->arguments->children.insert(keys->arguments->children.end(),
                    declare_index->index_columns->children.begin(), declare_index->index_columns->children.end());
        }
    }

    for (size_t index = 0; index < columns_define->children.size(); ++index)
    {
        const auto & declare_column = columns_define->children[index]->as<MySQLParser::ASTDeclareColumn>();

        if (declare_column->column_options)
        {
            if (const auto * options = declare_column->column_options->as<MySQLParser::ASTDeclareOptions>())
            {
                if (options->changes.count("unique_key"))
                    unique_keys->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(declare_column->name));

                if (options->changes.count("primary_key"))
                    primary_keys->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(declare_column->name));

                if (options->changes.contains("auto_increment"))
                    increment_columns.emplace(declare_column->name);
            }
        }
    }

    return std::make_tuple(
        getNames(*primary_keys, context, columns), getNames(*unique_keys, context, columns), getNames(*keys, context, columns), increment_columns);
}

static String getUniqueColumnName(NamesAndTypesList columns_name_and_type, const String & prefix)
{
    const auto & is_unique = [&](const String & column_name)
    {
        for (const auto & column_name_and_type : columns_name_and_type)
        {
            if (column_name_and_type.name == column_name)
                return false;
        }

        return true;
    };

    if (is_unique(prefix))
        return prefix;

    for (size_t index = 0; ; ++index)
    {
        const String & cur_name = prefix + "_" + toString(index);
        if (is_unique(cur_name))
            return cur_name;
    }
}

static ASTPtr getPartitionPolicy(const NamesAndTypesList & primary_keys)
{
    const auto & numbers_partition = [&](const String & column_name, const DataTypePtr & type, size_t type_max_size)
    {
        ASTPtr column = std::make_shared<ASTIdentifier>(column_name);

        if (type->isNullable())
            column = makeASTFunction("assumeNotNull", column);

        return makeASTFunction("divide", column, std::make_shared<ASTLiteral>(UInt64(type_max_size / 1000)));
    };

    for (const auto & primary_key : primary_keys)
    {
        WhichDataType which(primary_key.type);

        if (which.isNullable())
            which = WhichDataType((static_cast<const DataTypeNullable &>(*primary_key.type)).getNestedType());

        if (which.isDateOrDateTime())
        {
            ASTPtr res = std::make_shared<ASTIdentifier>(primary_key.name);
            return makeASTFunction("toYYYYMM", primary_key.type->isNullable() ? makeASTFunction("assumeNotNull", res) : res);
        }

        if (which.isInt8() || which.isUInt8())
            return std::make_shared<ASTIdentifier>(primary_key.name);
        else if (which.isInt16() || which.isUInt16())
            return numbers_partition(primary_key.name, primary_key.type, std::numeric_limits<UInt16>::max());
        else if (which.isInt32() || which.isUInt32())
            return numbers_partition(primary_key.name, primary_key.type, std::numeric_limits<UInt32>::max());
        else if (which.isInt64() || which.isUInt64())
            return numbers_partition(primary_key.name, primary_key.type, std::numeric_limits<UInt64>::max());
    }

    return {};
}

static ASTPtr getOrderByPolicy(
    const NamesAndTypesList & primary_keys, const NamesAndTypesList & unique_keys, const NamesAndTypesList & keys, const NameSet & increment_columns)
{
    NameSet order_by_columns_set;
    std::deque<String> order_by_columns;

    const auto & add_order_by_expression = [&](const NamesAndTypesList & names_and_types)
    {
        for (const auto & [name, type] : names_and_types)
        {
            if (order_by_columns_set.contains(name))
                continue;

            if (increment_columns.contains(name))
            {
                order_by_columns_set.emplace(name);
                order_by_columns.emplace_back(name);
            }
            else
            {
                order_by_columns_set.emplace(name);
                order_by_columns.emplace_front(name);
            }
        }
    };

    /// primary_key[not increment], key[not increment], unique[not increment], key[increment], unique[increment], primary_key[increment]
    add_order_by_expression(unique_keys);
    add_order_by_expression(keys);
    add_order_by_expression(primary_keys);

    auto order_by_expression = std::make_shared<ASTFunction>();
    order_by_expression->name = "tuple";
    order_by_expression->arguments = std::make_shared<ASTExpressionList>();

    for (const auto & order_by_column : order_by_columns)
        order_by_expression->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(order_by_column));

    return order_by_expression;
}

void InterpreterCreateImpl::validate(const InterpreterCreateImpl::TQuery & create_query, const Context &)
{
    /// This is dangerous, because the like table may not exists in ClickHouse
    if (create_query.like_table)
        throw Exception("Cannot convert create like statement to ClickHouse SQL", ErrorCodes::NOT_IMPLEMENTED);

    const auto & create_defines = create_query.columns_list->as<MySQLParser::ASTCreateDefines>();

    if (!create_defines || !create_defines->columns || create_defines->columns->children.empty())
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
}

ASTPtr InterpreterCreateImpl::getRewrittenQuery(
    const TQuery & create_query, const Context & context, const String & clickhouse_db, const String & filter_mysql_db)
{
    auto rewritten_query = std::make_shared<ASTCreateQuery>();
    const auto & database_name = context.resolveDatabase(create_query.database);

    if (database_name != filter_mysql_db)
        return {};

    const auto & create_defines = create_query.columns_list->as<MySQLParser::ASTCreateDefines>();

    NamesAndTypesList columns_name_and_type = getColumnsList(create_defines->columns);
    const auto & [primary_keys, unique_keys, keys, increment_columns] = getKeys(create_defines->columns, create_defines->indices, context, columns_name_and_type);

    if (primary_keys.empty())
        throw Exception("The " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(create_query.table)
            + " cannot be materialized, because there is no primary keys.", ErrorCodes::NOT_IMPLEMENTED);

    auto columns = std::make_shared<ASTColumns>();

    /// Add _sign and _version column.
    String sign_column_name = getUniqueColumnName(columns_name_and_type, "_sign");
    String version_column_name = getUniqueColumnName(columns_name_and_type, "_version");
    columns_name_and_type.emplace_back(NameAndTypePair{sign_column_name, std::make_shared<DataTypeInt8>()});
    columns_name_and_type.emplace_back(NameAndTypePair{version_column_name, std::make_shared<DataTypeUInt64>()});
    columns->set(columns->columns, InterpreterCreateQuery::formatColumns(columns_name_and_type));

    auto storage = std::make_shared<ASTStorage>();

    /// The `partition by` expression must use primary keys, otherwise the primary keys will not be merge.
    if (ASTPtr partition_expression = getPartitionPolicy(primary_keys))
        storage->set(storage->partition_by, partition_expression);

    /// The `order by` expression must use primary keys, otherwise the primary keys will not be merge.
    if (ASTPtr order_by_expression = getOrderByPolicy(primary_keys, unique_keys, keys, increment_columns))
        storage->set(storage->order_by, order_by_expression);

    storage->set(storage->engine, makeASTFunction("ReplacingMergeTree", std::make_shared<ASTIdentifier>(version_column_name)));

    rewritten_query->database = clickhouse_db;
    rewritten_query->table = create_query.table;
    rewritten_query->if_not_exists = create_query.if_not_exists;
    rewritten_query->set(rewritten_query->storage, storage);
    rewritten_query->set(rewritten_query->columns_list, columns);

    return rewritten_query;
}

void InterpreterDropImpl::validate(const InterpreterDropImpl::TQuery & /*query*/, const Context & /*context*/)
{
}

ASTPtr InterpreterDropImpl::getRewrittenQuery(
    const InterpreterDropImpl::TQuery & drop_query, const Context & context, const String & clickhouse_db, const String & filter_mysql_db)
{
    const auto & database_name = context.resolveDatabase(drop_query.database);

    if (database_name != filter_mysql_db)
        return {};

    ASTPtr rewritten_query = drop_query.clone();
    rewritten_query->as<ASTDropQuery>()->database = clickhouse_db;
    return rewritten_query;
}

void InterpreterRenameImpl::validate(const InterpreterRenameImpl::TQuery & rename_query, const Context & /*context*/)
{
    if (rename_query.exchange)
        throw Exception("Cannot execute exchange for external ddl query.", ErrorCodes::NOT_IMPLEMENTED);
}

ASTPtr InterpreterRenameImpl::getRewrittenQuery(
    const InterpreterRenameImpl::TQuery & rename_query, const Context & context, const String & clickhouse_db, const String & filter_mysql_db)
{
    ASTRenameQuery::Elements elements;
    for (const auto & rename_element : rename_query.elements)
    {
        const auto & to_database = context.resolveDatabase(rename_element.to.database);
        const auto & from_database = context.resolveDatabase(rename_element.from.database);

        if (to_database != from_database)
            throw Exception("Cannot rename with other database for external ddl query.", ErrorCodes::NOT_IMPLEMENTED);

        if (from_database == filter_mysql_db)
        {
            elements.push_back(ASTRenameQuery::Element());
            elements.back().from.database = clickhouse_db;
            elements.back().from.table = rename_element.from.table;
            elements.back().to.database = clickhouse_db;
            elements.back().to.table = rename_element.to.table;
        }
    }

    if (elements.empty())
        return ASTPtr{};

    auto rewritten_query = std::make_shared<ASTRenameQuery>();
    rewritten_query->elements = elements;
    return rewritten_query;
}

}

}
