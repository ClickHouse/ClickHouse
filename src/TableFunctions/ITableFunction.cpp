#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>
#include <Access/AccessFlags.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event TableFunctionExecute;
}

namespace DB
{

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    context.checkAccess(AccessType::CREATE_TEMPORARY_TABLE | StorageFactory::instance().getSourceAccessType(getStorageTypeName()));
    return executeImpl(ast_function, context, table_name);
}

}
