#pragma once

#include "config.h"

#if USE_HDFS

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Storages/ExternalDataSourceConfiguration.h>


namespace DB
{

class Context;

/* hdfs(URI, format[, structure, compression]) - creates a temporary storage from hdfs files
 *
 */
class TableFunctionHDFS : public ITableFunction
{
public:
    static constexpr auto name = "hdfs";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return configuration.structure != "auto"; }

    bool needStructureHint() const override { return configuration.structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "HDFS"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    URLBasedDataSourceConfiguration configuration;

    ColumnsDescription structure_hint;
    mutable std::optional<StorageHDFS::ObjectInfos> object_infos;
};

}

#endif
