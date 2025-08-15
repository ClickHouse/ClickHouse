#pragma once

#include <Storages/StorageFile.h>
#include <Storages/StorageFileCluster.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFile.h>
#include <TableFunctions/ITableFunctionCluster.h>

namespace DB
{

class Context;

class TableFunctionFileCluster : public ITableFunctionCluster<TableFunctionFile>
{
public:
    static constexpr auto name = "fileCluster";
    static constexpr auto signature = " - cluster, filename\n"
                                      " - cluster, filename, format\n"
                                      " - cluster, filename, format, structure\n"
                                      " - cluster, filename, format, structure, compression_method\n";

    String getName() const override { return name; }

    String getSignature() const override { return signature; }

protected:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_) const override;

    const char * getStorageTypeName() const override { return "FileCluster"; }
};

}
