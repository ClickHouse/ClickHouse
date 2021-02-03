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

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, const Context & context, const std::string & table_name,
                                   ColumnsDescription cached_columns) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    context.checkAccess(AccessType::CREATE_TEMPORARY_TABLE | StorageFactory::instance().getSourceAccessType(getStorageTypeName()));

    if (cached_columns.empty() || (hasStaticStructure() && cached_columns == getActualTableStructure(context)))
        return executeImpl(ast_function, context, table_name, std::move(cached_columns));

    auto get_storage = [=, tf = shared_from_this()]() -> StoragePtr
    {
        return tf->executeImpl(ast_function, context, table_name, cached_columns);
    };

    /// It will request actual table structure and create underlying storage lazily
    return std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage),
                                                       std::move(cached_columns), needStructureConversion());
}

}
