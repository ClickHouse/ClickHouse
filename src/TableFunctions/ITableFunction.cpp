#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTableFunction.h>
#include <Access/AccessFlags.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event TableFunctionExecute;
}

namespace DB
{

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
                                   ColumnsDescription cached_columns) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    context->checkAccess(AccessType::CREATE_TEMPORARY_TABLE | StorageFactory::instance().getSourceAccessType(getStorageTypeName()));

    if (cached_columns.empty())
        return executeImpl(ast_function, context, table_name, std::move(cached_columns));

    /// We have table structure, so it's CREATE AS table_function().
    /// We should use global context here because there will be no query context on server startup
    /// and because storage lifetime is bigger than query context lifetime.
    auto global_context = context->getGlobalContext();
    if (hasStaticStructure() && cached_columns == getActualTableStructure(context))
        return executeImpl(ast_function, global_context, table_name, std::move(cached_columns));

    auto this_table_function = shared_from_this();
    auto get_storage = [=]() -> StoragePtr
    {
        return this_table_function->executeImpl(ast_function, global_context, table_name, cached_columns);
    };

    /// It will request actual table structure and create underlying storage lazily
    return std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage),
                                                       std::move(cached_columns), needStructureConversion());
}

}
