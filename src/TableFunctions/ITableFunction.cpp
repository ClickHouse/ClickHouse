#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event TableFunctionExecute;
}

namespace DB
{

AccessType ITableFunction::getSourceAccessType() const
{
    return StorageFactory::instance().getSourceAccessType(getStorageTypeName());
}

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
                                   ColumnsDescription cached_columns, bool use_global_context) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    context->checkAccess(AccessType::CREATE_TEMPORARY_TABLE | getSourceAccessType());

    auto context_to_use = use_global_context ? context->getGlobalContext() : context;

    if (cached_columns.empty())
        return executeImpl(ast_function, context, table_name, std::move(cached_columns));

    if (hasStaticStructure() && cached_columns == getActualTableStructure(context))
        return executeImpl(ast_function, context_to_use, table_name, std::move(cached_columns));

    auto this_table_function = shared_from_this();
    auto get_storage = [=]() -> StoragePtr
    {
        return this_table_function->executeImpl(ast_function, context_to_use, table_name, cached_columns);
    };

    /// It will request actual table structure and create underlying storage lazily
    return std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage),
                                                       std::move(cached_columns), needStructureConversion());
}

}
