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

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns_) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    context.checkAccess(AccessType::CREATE_TEMPORARY_TABLE | StorageFactory::instance().getSourceAccessType(getStorageTypeName()));
    cached_columns = std::move(cached_columns_);

    bool no_conversion_required = hasStaticStructure() && cached_columns == getActualTableStructure(ast_function, context);
    if (cached_columns.empty() || no_conversion_required)
        return executeImpl(ast_function, context, table_name);

    auto get_storage = [=, tf = shared_from_this()]() -> StoragePtr
    {
        return tf->executeImpl(ast_function, context, table_name);
    };

    return std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage), cached_columns);
}

}
