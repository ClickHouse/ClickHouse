#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Access/Common/AccessFlags.h>
#    include <Formats/FormatFactory.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/parseColumnsListForTableFunction.h>
#    include <Storages/IStorage.h>
#    include <TableFunctions/ITableFunction.h>

namespace DB
{

template <typename Name, typename Storage, typename TableFunction>
class ITableFunctionDataLake : public TableFunction
{
public:
    static constexpr auto name = Name::name;
    std::string getName() const override { return name; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & /*ast_function*/,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription /*cached_columns*/,
        bool /*is_insert_query*/) const override
    {
        ColumnsDescription columns;
        if (TableFunction::configuration.structure != "auto")
            columns = parseColumnsListFromString(TableFunction::configuration.structure, context);

        StoragePtr storage = Storage::create(
            TableFunction::configuration, context, LoadingStrictnessLevel::CREATE, StorageID(TableFunction::getDatabaseName(), table_name),
            columns, ConstraintsDescription{}, String{}, std::nullopt);

        storage->startup();
        return storage;
    }

    const char * getStorageTypeName() const override { return Storage::name; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const override
    {
        if (TableFunction::configuration.structure == "auto")
        {
            context->checkAccess(TableFunction::getSourceAccessType());
            return Storage::getTableStructureFromData(TableFunction::configuration, std::nullopt, context);
        }

        return parseColumnsListFromString(TableFunction::configuration.structure, context);
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override
    {
        /// Set default format to Parquet if it's not specified in arguments.
        TableFunction::configuration.format = "Parquet";
        TableFunction::parseArguments(ast_function, context);
    }
};
}

#endif
