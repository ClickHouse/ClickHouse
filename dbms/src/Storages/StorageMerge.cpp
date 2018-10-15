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
#include <Interpreters/SettingsCommon.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <ext/range.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_PREWHERE;
    extern const int INCOMPATIBLE_SOURCE_TABLES;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
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

    DatabasePtr database = context.getDatabase(source_database);
    DatabaseIteratorPtr iterator = database->getIterator(context);

    size_t selected_table_size = 0;

    while (iterator->isValid())
    {
        if (table_name_regexp.match(iterator->name()))
        {
            auto & table = iterator->table();
            if (table.get() != this)
            {
                ++selected_table_size;
                stage_in_source_tables = std::max(stage_in_source_tables, table->getQueryProcessingStage(context));
            }

        }

        iterator->next();
    }

    return selected_table_size == 1 ? stage_in_source_tables : std::min(stage_in_source_tables, QueryProcessingStage::WithMergeableState);
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

    /** Just in case, turn off optimization "transfer to PREWHERE",
      * since there is no certainty that it works when one of table is MergeTree and other is not.
      */
    Context modified_context = context;
    modified_context.getSettingsRef().optimize_move_to_prewhere = false;

    /// What will be result structure depending on query processed stage in source tables?
    Block header = getQueryHeader(column_names, query_info, context, processed_stage);

    /** First we make list of selected tables to find out its size.
      * This is necessary to correctly pass the recommended number of threads to each table.
      */
    StorageListWithLocks selected_tables = getSelectedTables(query_info.query, has_table_virtual_column, true);

    if (selected_tables.empty())
        return createSourceStreams(
            query_info, processed_stage, max_block_size, header, {}, {}, real_column_names, modified_context, 0, has_table_virtual_column);

    size_t remaining_streams = num_streams;
    size_t tables_count = selected_tables.size();

    for (auto it = selected_tables.begin(); it != selected_tables.end(); ++it)
    {
        size_t current_need_streams =  tables_count >= num_streams ? 1 : (num_streams / tables_count);
        size_t current_streams = std::min(current_need_streams, remaining_streams);
        remaining_streams -= current_streams;
        current_streams = std::max(1, current_streams);

        StoragePtr storage = it->first;
        TableStructureReadLockPtr struct_lock = it->second;

        BlockInputStreams source_streams;

        if (current_streams)
        {
            source_streams = createSourceStreams(
                query_info, processed_stage, max_block_size, header, storage,
                struct_lock, real_column_names, modified_context, current_streams, has_table_virtual_column);
        }
        else
        {
            source_streams.emplace_back(std::make_shared<LazyBlockInputStream>(
                header, [=, &real_column_names, &modified_context]() -> BlockInputStreamPtr
                {
                    BlockInputStreams streams = createSourceStreams(query_info, processed_stage, max_block_size,
                                                                    header, storage, struct_lock, real_column_names,
                                                                    modified_context, current_streams, has_table_virtual_column, true);

                    if (!streams.empty() && streams.size() != 1)
                        throw Exception("LogicalError: the lazy stream size must to be one or empty.", ErrorCodes::LOGICAL_ERROR);

                    return streams.empty() ? std::make_shared<NullBlockInputStream>(header) : streams[0];
                }));
        }

        res.insert(res.end(), source_streams.begin(), source_streams.end());
    }

    if (res.empty())
        return res;

    res = narrowBlockInputStreams(res, num_streams);
    return res;
}

BlockInputStreams StorageMerge::createSourceStreams(const SelectQueryInfo & query_info, const QueryProcessingStage::Enum & processed_stage,
                                                    const size_t max_block_size, const Block & header, const StoragePtr & storage,
                                                    const TableStructureReadLockPtr & struct_lock, Names & real_column_names,
                                                    Context & modified_context, size_t streams_num, bool has_table_virtual_column,
                                                    bool concat_streams)
{
    SelectQueryInfo modified_query_info;
    modified_query_info.sets = query_info.sets;
    modified_query_info.query = query_info.query->clone();
    modified_query_info.prewhere_info = query_info.prewhere_info;

    VirtualColumnUtils::rewriteEntityInAst(modified_query_info.query, "_table", storage ? storage->getTableName() : "");

    if (!storage)
        return BlockInputStreams{
            InterpreterSelectQuery(modified_query_info.query, modified_context, std::make_shared<OneBlockInputStream>(header),
                                   processed_stage, true).execute().in};

    BlockInputStreams source_streams;

    if (processed_stage <= storage->getQueryProcessingStage(modified_context))
    {
        /// If there are only virtual columns in query, you must request at least one other column.
        if (real_column_names.size() ==0)
            real_column_names.push_back(ExpressionActions::getSmallestColumn(storage->getColumns().getAllPhysical()));

        source_streams = storage->read(real_column_names, modified_query_info, modified_context, processed_stage, max_block_size,
                                       UInt32(streams_num));
    }
    else if (processed_stage > storage->getQueryProcessingStage(modified_context))
    {
        typeid_cast<ASTSelectQuery *>(modified_query_info.query.get())->replaceDatabaseAndTable(source_database, storage->getTableName());

        /// Maximum permissible parallelism is streams_num
        modified_context.getSettingsRef().max_threads = UInt64(streams_num);
        modified_context.getSettingsRef().max_streams_to_max_threads_ratio = 1;

        InterpreterSelectQuery interpreter{modified_query_info.query, modified_context, Names{}, processed_stage};
        BlockInputStreamPtr interpreter_stream = interpreter.execute().in;

        /** Materialization is needed, since from distributed storage the constants come materialized.
          * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
          * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
          */
        source_streams.emplace_back(std::make_shared<MaterializingBlockInputStream>(interpreter_stream));
    }

    if (!source_streams.empty())
    {
        if (concat_streams)
        {
            BlockInputStreamPtr stream =
                source_streams.size() > 1 ? std::make_shared<ConcatBlockInputStream>(source_streams) : source_streams[0];

            source_streams.resize(1);
            source_streams[0] = stream;
        }

        for (BlockInputStreamPtr & source_stream : source_streams)
        {
            if (has_table_virtual_column)
                source_stream = std::make_shared<AddingConstColumnBlockInputStream<String>>(
                    source_stream, std::make_shared<DataTypeString>(), storage->getTableName(), "_table");

            /// Subordinary tables could have different but convertible types, like numeric types of different width.
            /// We must return streams with structure equals to structure of Merge table.
            convertingSourceStream(header, modified_context, modified_query_info.query, source_stream, processed_stage);

            source_stream->addTableLock(struct_lock);
        }
    }

    return source_streams;
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


StorageMerge::StorageListWithLocks StorageMerge::getSelectedTables(const ASTPtr & query, bool has_virtual_column, bool get_lock) const
{
    StorageListWithLocks selected_tables;
    DatabasePtr database = context.getDatabase(source_database);
    DatabaseIteratorPtr iterator = database->getIterator(context);

    auto virtual_column = ColumnString::create();

    while (iterator->isValid())
    {
        if (table_name_regexp.match(iterator->name()))
        {
            StoragePtr storage = iterator->table();

            if (query && typeid_cast<ASTSelectQuery *>(query.get())->prewhere_expression && !storage->supportsPrewhere())
                throw Exception("Storage " + storage->getName() + " doesn't support PREWHERE.", ErrorCodes::ILLEGAL_PREWHERE);

            if (storage.get() != this)
            {
                virtual_column->insert(storage->getTableName());
                selected_tables.emplace_back(storage, get_lock ? storage->lockStructure(false, __PRETTY_FUNCTION__) : TableStructureReadLockPtr{});
            }
        }

        iterator->next();
    }

    if (has_virtual_column)
    {
        Block virtual_columns_block = Block{ColumnWithTypeAndName(std::move(virtual_column), std::make_shared<DataTypeString>(), "_table")};
        VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);
        auto values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_table");

        /// Remove unused tables from the list
        selected_tables.remove_if([&] (const auto & elem) { return values.find(elem.first->getTableName()) == values.end(); });
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

Block StorageMerge::getQueryHeader(
    const Names & column_names, const SelectQueryInfo & query_info, const Context & context, QueryProcessingStage::Enum processed_stage)
{
    switch (processed_stage)
    {
        case QueryProcessingStage::FetchColumns:
        {
            Block header = getSampleBlockForColumns(column_names);
            if (query_info.prewhere_info)
            {
                query_info.prewhere_info->prewhere_actions->execute(header);
                header = materializeBlock(header);
                if (query_info.prewhere_info->remove_prewhere_column)
                    header.erase(query_info.prewhere_info->prewhere_column_name);
            }
            return header;
        }
        case QueryProcessingStage::WithMergeableState:
        case QueryProcessingStage::Complete:
            return materializeBlock(InterpreterSelectQuery(
                query_info.query, context, std::make_shared<OneBlockInputStream>(getSampleBlockForColumns(column_names)),
                                       processed_stage, true).getSampleBlock());
    }
    throw Exception("Logical Error: unknown processed stage.", ErrorCodes::LOGICAL_ERROR);
}

void StorageMerge::convertingSourceStream(const Block & header, const Context & context, ASTPtr & query,
                                          BlockInputStreamPtr & source_stream, QueryProcessingStage::Enum processed_stage)
{
    Block before_block_header = source_stream->getHeader();
    source_stream = std::make_shared<ConvertingBlockInputStream>(context, source_stream, header, ConvertingBlockInputStream::MatchColumnsMode::Name);

    ASTPtr where_expression = typeid_cast<ASTSelectQuery *>(query.get())->where_expression;

    if (!where_expression)
        return;

    for (size_t column_index : ext::range(0, header.columns()))
    {
        ColumnWithTypeAndName header_column = header.getByPosition(column_index);
        ColumnWithTypeAndName before_column = before_block_header.getByName(header_column.name);
        /// If the processed_stage greater than FetchColumns and the block structure between streams is different.
        /// the where expression maybe invalid because of convertingBlockInputStream.
        /// So we need to throw exception.
        if (!header_column.type->equals(*before_column.type.get()) && processed_stage > QueryProcessingStage::FetchColumns)
        {
            NamesAndTypesList source_columns = getSampleBlock().getNamesAndTypesList();
            NameAndTypePair virtual_column = getColumn("_table");
            source_columns.insert(source_columns.end(), virtual_column);
            ExpressionActionsPtr actions = ExpressionAnalyzer{where_expression, context, {}, source_columns}.getActions(false, false);
            Names required_columns = actions->getRequiredColumns();

            for (const auto required_column : required_columns)
            {
                if (required_column == header_column.name)
                    throw Exception("Block structure mismatch in Merge Storage: different types:\n" + before_block_header.dumpStructure()
                                    + "\n" + header.dumpStructure(), ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
            }
        }

    }
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
