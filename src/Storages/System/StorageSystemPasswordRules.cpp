#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Storages/System/StorageSystemPasswordRules.h>


namespace DB
{

NamesAndTypesList StorageSystemPasswordRules::getNamesAndTypes()
{
    return {
        {"pattern", std::make_shared<DataTypeString>()},
        {"message", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemPasswordRules::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    for (const auto & [original_pattern, exception_message] : context->getAccessControl().getPasswordComplexityRules())
    {
        res_columns[0]->insert(original_pattern);
        res_columns[1]->insert(exception_message);
    }
}

}
