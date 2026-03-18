#pragma once

#include <optional>

#include <Storages/StorageFuzzJSON.h>
#include <TableFunctions/ITableFunction.h>

#include "config.h"

#if USE_RAPIDJSON || USE_SIMDJSON
namespace DB
{

class TableFunctionFuzzJSON : public ITableFunction
{
public:
    static constexpr auto name = "fuzzJSON";
    std::string getName() const override { return name; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "FuzzJSON"; }

    String source;
    std::optional<UInt64> random_seed;
    std::optional<bool> should_reuse_output;
    std::optional<UInt64> max_output_length;

    StorageFuzzJSON::Configuration configuration;
};

}
#endif
