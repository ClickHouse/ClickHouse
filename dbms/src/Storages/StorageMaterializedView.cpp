#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>

#include <Storages/StorageMaterializedView.h>
#include <Storages/VirtualColumnFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StoragePtr StorageMaterializedView::create(
    const String & table_name_,
    const String & database_name_,
    Context & context_,
    ASTPtr & query_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    bool attach_)
{
    return ext::shared_ptr_helper<StorageMaterializedView>::make_shared(
        table_name_, database_name_, context_, query_, columns_,
        materialized_columns_, alias_columns_, column_defaults_, attach_
    );
}

StorageMaterializedView::StorageMaterializedView(
    const String & table_name_,
    const String & database_name_,
    Context & context_,
    ASTPtr & query_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    bool attach_)
    : StorageView{table_name_, database_name_, context_, query_, columns_, materialized_columns_, alias_columns_, column_defaults_}
{
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_);

    auto inner_table_name = getInnerTableName();

    /// If there is an ATTACH request, then the internal table must already be connected.
    if (!attach_)
    {
        /// We will create a query to create an internal repository.
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->database = database_name;
        manual_create_query->table = inner_table_name;
        manual_create_query->columns = create.columns;
        manual_create_query->children.push_back(manual_create_query->columns);

        /// If you do not specify a storage type in the query, try retrieving it from SELECT query.
        if (!create.inner_storage)
        {
            /// TODO also try to extract `params` to create a repository
            auto func = std::make_shared<ASTFunction>();
            func->name = context.getTable(select_database_name, select_table_name)->getName();
            manual_create_query->storage = func;
        }
        else
            manual_create_query->storage = create.inner_storage;

        manual_create_query->children.push_back(manual_create_query->storage);

        /// Execute the query.
        try
        {
            InterpreterCreateQuery create_interpreter(manual_create_query, context);
            create_interpreter.execute();
        }
        catch (...)
        {
            /// In case of any error we should remove dependency to the view
            /// which was added in the constructor of StorageView.
            if (!select_table_name.empty())
                context.getGlobalContext().removeDependency(
                    DatabaseAndTableName(select_database_name, select_table_name),
                    DatabaseAndTableName(database_name, table_name));

            throw;
        }
    }
}

NameAndTypePair StorageMaterializedView::getColumn(const String & column_name) const
{
    auto type = VirtualColumnFactory::tryGetType(column_name);
    if (type)
        return NameAndTypePair(column_name, type);

    return getRealColumn(column_name);
}

bool StorageMaterializedView::hasColumn(const String & column_name) const
{
    return VirtualColumnFactory::hasColumn(column_name) || IStorage::hasColumn(column_name);
}

BlockInputStreams StorageMaterializedView::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    return getInnerTable()->read(column_names, query, context, settings, processed_stage, max_block_size, threads);
}

BlockOutputStreamPtr StorageMaterializedView::write(ASTPtr query, const Settings & settings)
{
    return getInnerTable()->write(query, settings);
}

void StorageMaterializedView::drop()
{
    context.getGlobalContext().removeDependency(
        DatabaseAndTableName(select_database_name, select_table_name),
        DatabaseAndTableName(database_name, table_name));

    auto inner_table_name = getInnerTableName();

    if (context.tryGetTable(database_name, inner_table_name))
    {
        /// We create and execute `drop` query for internal repository.
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = database_name;
        drop_query->table = inner_table_name;
        ASTPtr ast_drop_query = drop_query;
        InterpreterDropQuery drop_interpreter(ast_drop_query, context);
        drop_interpreter.execute();
    }
}

bool StorageMaterializedView::optimize(const String & partition, bool final, bool deduplicate, const Settings & settings)
{
    return getInnerTable()->optimize(partition, final, deduplicate, settings);
}

StoragePtr StorageMaterializedView::getInnerTable() const
{
    return context.getTable(database_name, getInnerTableName());
}


}
