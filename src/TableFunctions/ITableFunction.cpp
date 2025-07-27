#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Common/ProfileEvents.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/Context.h>


namespace ProfileEvents
{
    extern const Event TableFunctionExecute;
}

namespace DB
{

std::optional<AccessTypeObjects::Source> ITableFunction::getSourceAccessObject() const
{
    return StorageFactory::instance().getSourceAccessObject(getStorageTypeName());
}

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
                                   ColumnsDescription cached_columns, bool use_global_context, bool is_insert_query) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);

    if (const auto access_object = getSourceAccessObject())
    {
        if (is_insert_query)
            context->checkAccess(AccessType::WRITE, toStringSource(*access_object));
        else
            context->checkAccess(AccessType::READ, toStringSource(*access_object));
    }

    auto table_function_properties = TableFunctionFactory::instance().tryGetProperties(getName());
    if (is_insert_query || !(table_function_properties && table_function_properties->allow_readonly))
        context->checkAccess(AccessType::CREATE_TEMPORARY_TABLE);

    auto context_to_use = use_global_context ? context->getGlobalContext() : context;

    if (cached_columns.empty())
        return executeImpl(ast_function, context, table_name, std::move(cached_columns), is_insert_query);

    if (hasStaticStructure() && cached_columns == getActualTableStructure(context, is_insert_query))
        return executeImpl(ast_function, context_to_use, table_name, std::move(cached_columns), is_insert_query);

    auto this_table_function = shared_from_this();
    auto get_storage = [=]() -> StoragePtr
    {
        return this_table_function->executeImpl(ast_function, context_to_use, table_name, cached_columns, is_insert_query);
    };

    /// It will request actual table structure and create underlying storage lazily
    return std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage),
                                                       std::move(cached_columns), needStructureConversion());
}

}
