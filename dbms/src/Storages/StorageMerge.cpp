#include <DataStreams/AddingConstColumnBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <Storages/StorageMerge.h>
#include <Common/VirtualColumnUtils.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/VirtualColumnFactory.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Databases/IDatabase.h>
#include <DataStreams/CastTypeBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_PREWHERE;
    extern const int INCOMPATIBLE_SOURCE_TABLES;
}


StorageMerge::StorageMerge(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const String & source_database_,
    const String & table_name_regexp_,
    const Context & context_)
    : name(name_), columns(columns_), source_database(source_database_),
      table_name_regexp(table_name_regexp_), context(context_)
{
}

StorageMerge::StorageMerge(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const String & source_database_,
    const String & table_name_regexp_,
    const Context & context_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
    name(name_), columns(columns_), source_database(source_database_),
    table_name_regexp(table_name_regexp_), context(context_)
{
}

StoragePtr StorageMerge::create(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const String & source_database_,
    const String & table_name_regexp_,
    const Context & context_)
{
    return make_shared(
        name_, columns_,
        source_database_, table_name_regexp_, context_
    );
}

StoragePtr StorageMerge::create(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const String & source_database_,
    const String & table_name_regexp_,
    const Context & context_)
{
    return make_shared(
        name_, columns_, materialized_columns_, alias_columns_, column_defaults_,
        source_database_, table_name_regexp_, context_
    );
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

BlockInputStreams StorageMerge::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    BlockInputStreams res;

    Names virt_column_names, real_column_names;
    for (const auto & it : column_names)
        if (it != "_table")
            real_column_names.push_back(it);
        else
            virt_column_names.push_back(it);

    std::experimental::optional<QueryProcessingStage::Enum> processed_stage_in_source_tables;

    /** First we make list of selected tables to find out its size.
      * This is necessary to correctly pass the recommended number of threads to each table.
      */
    StorageListWithLocks selected_tables = getSelectedTables();

    /// If PREWHERE is used in query, you need to make sure that all tables support this.
    if (typeid_cast<const ASTSelectQuery &>(*query).prewhere_expression)
        for (const auto & elem : selected_tables)
            if (!elem.first->supportsPrewhere())
                throw Exception("Storage " + elem.first->getName() + " doesn't support PREWHERE.", ErrorCodes::ILLEGAL_PREWHERE);

    Block virtual_columns_block = getBlockWithVirtualColumns(selected_tables);

    /// If at least one virtual column is requested, try indexing
    if (!virt_column_names.empty())
    {
        VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);

        auto values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_table");

        /// Remove unused tables from the list
        selected_tables.remove_if([&] (const auto & elem) { return values.find(elem.first->getTableName()) == values.end(); });
    }

    /** Just in case, turn off optimization "transfer to PREWHERE",
      * since there is no certainty that it works when one of table is MergeTree and other is not.
      */
    Settings modified_settings = settings;
    modified_settings.optimize_move_to_prewhere = false;

    size_t tables_count = selected_tables.size();
    size_t curr_table_number = 0;
    for (auto it = selected_tables.begin(); it != selected_tables.end(); ++it, ++curr_table_number)
    {
        StoragePtr table = it->first;
        auto & table_lock = it->second;

        /// If there are only virtual columns in query, you must request at least one other column.
        if (real_column_names.size() == 0)
            real_column_names.push_back(ExpressionActions::getSmallestColumn(table->getColumnsList()));

        /// Substitute virtual column for its value
        ASTPtr modified_query_ast = query->clone();
        VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, "_table", table->getTableName());

        BlockInputStreams source_streams;

        if (curr_table_number < threads)
        {
            QueryProcessingStage::Enum processed_stage_in_source_table = processed_stage;
            source_streams = table->read(
                real_column_names,
                modified_query_ast,
                context,
                modified_settings,
                processed_stage_in_source_table,
                max_block_size,
                tables_count >= threads ? 1 : (threads / tables_count));

            if (!processed_stage_in_source_tables)
                processed_stage_in_source_tables.emplace(processed_stage_in_source_table);
            else if (processed_stage_in_source_table != processed_stage_in_source_tables.value())
                throw Exception("Source tables for Merge table are processing data up to different stages",
                    ErrorCodes::INCOMPATIBLE_SOURCE_TABLES);

            /// Subordinary tables could have different but convertible types, like numeric types of different width.
            /// We must return streams with structure equals to structure of Merge table. 
            for (auto & stream : source_streams)
                stream = std::make_shared<CastTypeBlockInputStream>(context, stream, table->getSampleBlock(), getSampleBlock());
        }
        else
        {
            /// If many streams, initialize it lazily, to avoid long delay before start of query processing.
            source_streams.emplace_back(std::make_shared<LazyBlockInputStream>([=]
            {
                QueryProcessingStage::Enum processed_stage_in_source_table = processed_stage;
                BlockInputStreams streams = table->read(
                    real_column_names,
                    modified_query_ast,
                    context,
                    modified_settings,
                    processed_stage_in_source_table,
                    max_block_size,
                    1);

                if (!processed_stage_in_source_tables)
                    throw Exception("Logical error: unknown processed stage in source tables",
                        ErrorCodes::LOGICAL_ERROR);
                else if (processed_stage_in_source_table != processed_stage_in_source_tables.value())
                    throw Exception("Source tables for Merge table are processing data up to different stages",
                        ErrorCodes::INCOMPATIBLE_SOURCE_TABLES);

                auto stream = streams.empty() ? std::make_shared<NullBlockInputStream>() : streams.front();
                if (!streams.empty())
                    stream = std::make_shared<CastTypeBlockInputStream>(context, stream, table->getSampleBlock(), getSampleBlock());
                return stream;
            }));
        }

        for (auto & stream : source_streams)
            stream->addTableLock(table_lock);

        for (auto & virtual_column : virt_column_names)
        {
            if (virtual_column == "_table")
            {
                for (auto & stream : source_streams)
                    stream = std::make_shared<AddingConstColumnBlockInputStream<String>>(
                        stream, std::make_shared<DataTypeString>(), table->getTableName(), "_table");
            }
        }

        res.insert(res.end(), source_streams.begin(), source_streams.end());
    }

    if (processed_stage_in_source_tables)
        processed_stage = processed_stage_in_source_tables.value();

    return narrowBlockInputStreams(res, threads);
}

/// Construct a block consisting only of possible values of virtual columns
Block StorageMerge::getBlockWithVirtualColumns(const StorageListWithLocks & selected_tables) const
{
    Block res;
    ColumnWithTypeAndName _table(std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "_table");

    for (const auto & elem : selected_tables)
        _table.column->insert(elem.first->getTableName());

    res.insert(_table);
    return res;
}

StorageMerge::StorageListWithLocks StorageMerge::getSelectedTables() const
{
    StorageListWithLocks selected_tables;
    auto database = context.getDatabase(source_database);
    auto iterator = database->getIterator();

    while (iterator->isValid())
    {
        if (table_name_regexp.match(iterator->name()))
        {
            auto & table = iterator->table();
            if (table.get() != this)
                selected_tables.emplace_back(table, table->lockStructure(false));
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

    auto lock = lockStructureForAlter();
    params.apply(*columns, materialized_columns, alias_columns, column_defaults);

    context.getDatabase(database_name)->alterTable(
        context, table_name,
        *columns, materialized_columns, alias_columns, column_defaults, {});
}

}
