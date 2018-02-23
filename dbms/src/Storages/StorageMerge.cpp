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
#include <Storages/VirtualColumnFactory.h>
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
}


StorageMerge::StorageMerge(
    const std::string & name_,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const String & source_database_,
    const String & table_name_regexp_,
    const Context & context_)
    : IStorage{columns_, materialized_columns_, alias_columns_, column_defaults_},
    name(name_), source_database(source_database_),
    table_name_regexp(table_name_regexp_), context(context_)
{
}


NameAndTypePair StorageMerge::getColumn(const String & column_name) const
{
    auto type = VirtualColumnFactory::tryGetType(column_name);
    if (type)
        return NameAndTypePair(column_name, type);

    return IStorage::getColumn(column_name);
}

bool StorageMerge::hasColumn(const String & column_name) const
{
    return VirtualColumnFactory::hasColumn(column_name) || IStorage::hasColumn(column_name);
}


bool StorageMerge::isRemote() const
{
    auto database = context.getDatabase(source_database);
    auto iterator = database->getIterator(context);

    while (iterator->isValid())
    {
        if (table_name_regexp.match(iterator->name()))
        {
            auto & table = iterator->table();
            if (table.get() != this && table->isRemote())
                return true;
        }

        iterator->next();
    }

    return false;
}


namespace
{
    using NodeHashToSet = std::map<IAST::Hash, SetPtr>;

    void relinkSetsImpl(const ASTPtr & query, const NodeHashToSet & node_hash_to_set, PreparedSets & new_sets)
    {
        auto hash = query->getTreeHash();
        auto it = node_hash_to_set.find(hash);
        if (node_hash_to_set.end() != it)
            new_sets[query.get()] = it->second;

        for (const auto & child : query->children)
            relinkSetsImpl(child, node_hash_to_set, new_sets);
    }

    /// Re-link prepared sets onto cloned and modified AST.
    void relinkSets(const ASTPtr & query, const PreparedSets & old_sets, PreparedSets & new_sets)
    {
        NodeHashToSet node_hash_to_set;
        for (const auto & node_set : old_sets)
            node_hash_to_set.emplace(node_set.first->getTreeHash(), node_set.second);

        relinkSetsImpl(query, node_hash_to_set, new_sets);
    }
}


BlockInputStreams StorageMerge::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
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

    std::optional<QueryProcessingStage::Enum> processed_stage_in_source_tables;

    /** First we make list of selected tables to find out its size.
      * This is necessary to correctly pass the recommended number of threads to each table.
      */
    StorageListWithLocks selected_tables = getSelectedTables();

    const ASTPtr & query = query_info.query;

    /// If PREWHERE is used in query, you need to make sure that all tables support this.
    if (typeid_cast<const ASTSelectQuery &>(*query).prewhere_expression)
        for (const auto & elem : selected_tables)
            if (!elem.first->supportsPrewhere())
                throw Exception("Storage " + elem.first->getName() + " doesn't support PREWHERE.", ErrorCodes::ILLEGAL_PREWHERE);

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
            real_column_names.push_back(ExpressionActions::getSmallestColumn(table->getColumnsList()));

        /// Substitute virtual column for its value when querying tables.
        ASTPtr modified_query_ast = query->clone();
        VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, "_table", table->getTableName());

        SelectQueryInfo modified_query_info;
        modified_query_info.query = modified_query_ast;

        relinkSets(modified_query_info.query, query_info.sets, modified_query_info.sets);

        BlockInputStreams source_streams;

        if (curr_table_number < num_streams)
        {
            QueryProcessingStage::Enum processed_stage_in_source_table = processed_stage;
            source_streams = table->read(
                real_column_names,
                modified_query_info,
                modified_context,
                processed_stage_in_source_table,
                max_block_size,
                tables_count >= num_streams ? 1 : (num_streams / tables_count));

            if (!processed_stage_in_source_tables)
                processed_stage_in_source_tables.emplace(processed_stage_in_source_table);
            else if (processed_stage_in_source_table != *processed_stage_in_source_tables)
                throw Exception("Source tables for Merge table are processing data up to different stages",
                    ErrorCodes::INCOMPATIBLE_SOURCE_TABLES);

            if (!header)
            {
                switch (processed_stage_in_source_table)
                {
                    case QueryProcessingStage::FetchColumns:
                        header = getSampleBlockForColumns(column_names);
                        break;
                    case QueryProcessingStage::WithMergeableState:
                        header = materializeBlock(InterpreterSelectQuery(query_info.query, context, QueryProcessingStage::WithMergeableState, 0,
                            std::make_shared<OneBlockInputStream>(getSampleBlockForColumns(column_names))).execute().in->getHeader());
                        break;
                    case QueryProcessingStage::Complete:
                        header = materializeBlock(InterpreterSelectQuery(query_info.query, context, QueryProcessingStage::Complete, 0,
                            std::make_shared<OneBlockInputStream>(getSampleBlockForColumns(column_names))).execute().in->getHeader());
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
            if (!processed_stage_in_source_tables)
                throw Exception("Logical error: unknown processed stage in source tables", ErrorCodes::LOGICAL_ERROR);

            /// If many streams, initialize it lazily, to avoid long delay before start of query processing.
            source_streams.emplace_back(std::make_shared<LazyBlockInputStream>(header, [=]() -> BlockInputStreamPtr
            {
                QueryProcessingStage::Enum processed_stage_in_source_table = processed_stage;
                BlockInputStreams streams = table->read(
                    real_column_names,
                    modified_query_info,
                    modified_context,
                    processed_stage_in_source_table,
                    max_block_size,
                    1);

                if (processed_stage_in_source_table != *processed_stage_in_source_tables)
                    throw Exception("Source tables for Merge table are processing data up to different stages",
                        ErrorCodes::INCOMPATIBLE_SOURCE_TABLES);

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

    if (processed_stage_in_source_tables)
        processed_stage = *processed_stage_in_source_tables;

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
    params.apply(columns, materialized_columns, alias_columns, column_defaults);

    context.getDatabase(database_name)->alterTable(
        context, table_name,
        columns, materialized_columns, alias_columns, column_defaults, {});
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
            args.materialized_columns, args.alias_columns, args.column_defaults,
            source_database, table_name_regexp, args.context);
    });
}

}
