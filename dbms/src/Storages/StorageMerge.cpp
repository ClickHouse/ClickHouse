#include <DataStreams/AddingConstColumnBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_PREWHERE;
    extern const int INCOMPATIBLE_SOURCE_TABLES;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


StorageMerge::StorageMerge(
    const std::string & name_,
    const ColumnsDescription & columns_,
    const String & source_database_,
    const String & table_name_regexp_,
    const Context & context_)
    : IStorage{columns_},
    name(name_), source_database(source_database_),
    table_name_regexp(table_name_regexp_), context(context_)
{
}


/// NOTE Structure of underlying tables as well as their set are not constant,
///  so the results of these methods may become obsolete after the call.

NameAndTypePair StorageMerge::getColumn(const String & column_name) const
{
    /// virtual column of the Merge table itself
    if (column_name == "_table")
        return { column_name, std::make_shared<DataTypeString>() };

    if (IStorage::hasColumn(column_name))
        return IStorage::getColumn(column_name);

    /// virtual (and real) columns of the underlying tables
    auto first_table = getFirstTable([](auto &&) { return true; });
    if (first_table)
        return first_table->getColumn(column_name);

    throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}

bool StorageMerge::hasColumn(const String & column_name) const
{
    if (column_name == "_table")
        return true;

    if (IStorage::hasColumn(column_name))
        return true;

    auto first_table = getFirstTable([](auto &&) { return true; });
    if (first_table)
        return first_table->hasColumn(column_name);

    return false;
}


template <typename F>
StoragePtr StorageMerge::getFirstTable(F && predicate) const
{
    auto database = context.getDatabase(source_database);
    auto iterator = database->getIterator(context);

    while (iterator->isValid())
    {
        if (table_name_regexp.match(iterator->name()))
        {
            auto & table = iterator->table();
            if (table.get() != this && predicate(table))
                return table;
        }

        iterator->next();
    }

    return {};
}


bool StorageMerge::isRemote() const
{
    auto first_remote_table = getFirstTable([](const StoragePtr & table) { return table->isRemote(); });
    return first_remote_table != nullptr;
}


bool StorageMerge::mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const
{
    /// It's beneficial if it is true for at least one table.
    StorageListWithLocks selected_tables = getSelectedTables();

    size_t i = 0;
    for (const auto & table : selected_tables)
    {
        if (table.first->mayBenefitFromIndexForIn(left_in_operand))
            return true;

        ++i;
        /// For simplicity reasons, check only first ten tables.
        if (i > 10)
            break;
    }

    return false;
}


QueryProcessingStage::Enum StorageMerge::getQueryProcessingStage(const Context & context) const
{
    auto stage_in_source_tables = QueryProcessingStage::FetchColumns;

    auto database = context.getDatabase(source_database);
    auto iterator = database->getIterator(context);

    size_t selected_table_size = 0;

    while (iterator->isValid())
    {
        if (table_name_regexp.match(iterator->name()))
        {
            auto & table = iterator->table();
            if (table.get() != this)
                ++selected_table_size;
        }

        iterator->next();
    }

    auto fetch_or_mergeable_stage = std::min(stage_in_source_tables, QueryProcessingStage::WithMergeableState);
    return selected_table_size == 1 ? stage_in_source_tables : fetch_or_mergeable_stage;
}


BlockInputStreams StorageMerge::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    BlockInputStreams res;

    bool has_table_virtual_column = false;
    Names real_column_names;
    real_column_names.reserve(column_names.size());

    for (const auto & name : column_names)
    {
        if (name == "_table")
        {
            has_table_virtual_column = true;
        }
        else
            real_column_names.push_back(name);
    }

    /** First we make list of selected tables to find out its size.
      * This is necessary to correctly pass the recommended number of threads to each table.
      */
    StorageListWithLocks selected_tables = getSelectedTables();

    const ASTPtr & query = query_info.query;

    for (const auto & elem : selected_tables)
    {
        /// If PREWHERE is used in query, you need to make sure that all tables support this.
        if (typeid_cast<const ASTSelectQuery &>(*query).prewhere_expression)
            if (!elem.first->supportsPrewhere())
                throw Exception("Storage " + elem.first->getName() + " doesn't support PREWHERE.",
                                ErrorCodes::ILLEGAL_PREWHERE);
    }

    Block virtual_columns_block = getBlockWithVirtualColumns(selected_tables);

    /// If _table column is requested, try filtering
    if (has_table_virtual_column)
    {
        VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);
        auto values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_table");

        /// Remove unused tables from the list
        selected_tables.remove_if([&] (const auto & elem) { return values.find(elem.first->getTableName()) == values.end(); });
    }

    /** Just in case, turn off optimization "transfer to PREWHERE",
      * since there is no certainty that it works when one of table is MergeTree and other is not.
      */
    Context modified_context = context;
    modified_context.getSettingsRef().optimize_move_to_prewhere = false;

    /// What will be result structure depending on query processed stage in source tables?
    Block header;

    size_t tables_count = selected_tables.size();

    size_t curr_table_number = 0;
    for (auto it = selected_tables.begin(); it != selected_tables.end(); ++it, ++curr_table_number)
    {
        StoragePtr table = it->first;
        auto & table_lock = it->second;

        /// If there are only virtual columns in query, you must request at least one other column.
        if (real_column_names.size() == 0)
            real_column_names.push_back(ExpressionActions::getSmallestColumn(table->getColumns().getAllPhysical()));

        /// Substitute virtual column for its value when querying tables.
        ASTPtr modified_query_ast = query->clone();
        VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, "_table", table->getTableName());

        SelectQueryInfo modified_query_info;
        modified_query_info.query = modified_query_ast;
        modified_query_info.prewhere_info = query_info.prewhere_info;
        modified_query_info.sets = query_info.sets;

        BlockInputStreams source_streams;

        if (curr_table_number < num_streams)
        {
            source_streams = table->read(
                real_column_names,
                modified_query_info,
                modified_context,
                processed_stage,
                max_block_size,
                tables_count >= num_streams ? 1 : (num_streams / tables_count));

            if (!header)
            {
                switch (processed_stage)
                {
                    case QueryProcessingStage::FetchColumns:
                    {
                        header = getSampleBlockForColumns(column_names);

                        if (query_info.prewhere_info)
                        {
                            query_info.prewhere_info->prewhere_actions->execute(header);
                            header = materializeBlock(header);
                            if (query_info.prewhere_info->remove_prewhere_column)
                                header.erase(query_info.prewhere_info->prewhere_column_name);
                        }

                        break;
                    }
                    case QueryProcessingStage::WithMergeableState:
                    case QueryProcessingStage::Complete:
                        header = materializeBlock(InterpreterSelectQuery(
                            query_info.query, context, std::make_shared<OneBlockInputStream>(getSampleBlockForColumns(column_names)),
                            processed_stage, true).getSampleBlock());
                        break;
                }
            }

            if (has_table_virtual_column)
                for (auto & stream : source_streams)
                    stream = std::make_shared<AddingConstColumnBlockInputStream<String>>(
                        stream, std::make_shared<DataTypeString>(), table->getTableName(), "_table");

            /// Subordinary tables could have different but convertible types, like numeric types of different width.
            /// We must return streams with structure equals to structure of Merge table.
            for (auto & stream : source_streams)
                stream = std::make_shared<ConvertingBlockInputStream>(context, stream, header, ConvertingBlockInputStream::MatchColumnsMode::Name);
        }
        else
        {
            /// If many streams, initialize it lazily, to avoid long delay before start of query processing.
            source_streams.emplace_back(std::make_shared<LazyBlockInputStream>(header, [=]() -> BlockInputStreamPtr
            {
                BlockInputStreams streams = table->read(
                    real_column_names,
                    modified_query_info,
                    modified_context,
                    processed_stage,
                    max_block_size,
                    1);

                if (streams.empty())
                {
                    return std::make_shared<NullBlockInputStream>(header);
                }
                else
                {
                    BlockInputStreamPtr stream = streams.size() > 1 ? std::make_shared<ConcatBlockInputStream>(streams) : streams[0];

                    if (has_table_virtual_column)
                        stream = std::make_shared<AddingConstColumnBlockInputStream<String>>(
                            stream, std::make_shared<DataTypeString>(), table->getTableName(), "_table");

                    return std::make_shared<ConvertingBlockInputStream>(context, stream, header, ConvertingBlockInputStream::MatchColumnsMode::Name);
                }
            }));
        }

        for (auto & stream : source_streams)
            stream->addTableLock(table_lock);

        res.insert(res.end(), source_streams.begin(), source_streams.end());
    }

    if (res.empty())
        return res;

    res = narrowBlockInputStreams(res, num_streams);
    return res;
}

/// Construct a block consisting only of possible values of virtual columns
Block StorageMerge::getBlockWithVirtualColumns(const StorageListWithLocks & selected_tables) const
{
    auto column = ColumnString::create();

    for (const auto & elem : selected_tables)
        column->insert(elem.first->getTableName());

    return Block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "_table")};
}

StorageMerge::StorageListWithLocks StorageMerge::getSelectedTables() const
{
    StorageListWithLocks selected_tables;
    auto database = context.getDatabase(source_database);
    auto iterator = database->getIterator(context);

    while (iterator->isValid())
    {
        if (table_name_regexp.match(iterator->name()))
        {
            auto & table = iterator->table();
            if (table.get() != this)
                selected_tables.emplace_back(table, table->lockStructure(false, __PRETTY_FUNCTION__));
        }

        iterator->next();
    }

    return selected_tables;
}


void StorageMerge::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
    for (const auto & param : params)
        if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
            throw Exception("Storage engine " + getName() + " doesn't support primary key.", ErrorCodes::NOT_IMPLEMENTED);

    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    ColumnsDescription new_columns = getColumns();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, {});
    setColumns(new_columns);
}


void registerStorageMerge(StorageFactory & factory)
{
    factory.registerStorage("Merge", [](const StorageFactory::Arguments & args)
    {
        /** In query, the name of database is specified as table engine argument which contains source tables,
          *  as well as regex for source-table names.
          */

        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception("Storage Merge requires exactly 2 parameters"
                " - name of source database and regexp for table names.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.local_context);

        String source_database = static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>();
        String table_name_regexp = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();

        return StorageMerge::create(
            args.table_name, args.columns,
            source_database, table_name_regexp, args.context);
    });
}

}
