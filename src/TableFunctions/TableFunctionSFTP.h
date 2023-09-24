#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB
{

/* file(path, format[, structure, compression]) - creates a temporary storage from file
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionSFTP : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "sftp";
    static constexpr auto signature = " - url\n"
                                      " - url, format\n"
                                      " - url, format, structure\n"
                                      " - url, format, structure, compression_method\n"
                                      " - url, password, format\n"
                                      " - url, password, format, structure\n"
                                      " - url, password, format, structure, compression_method\n"
                                      "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    static size_t getMaxNumberOfArguments() { return 6; }

    std::string getName() const override
    {
        return name;
    }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override
    {
        return {"_path", "_file"};
    }

protected:
    void parseFirstArguments(const ASTPtr & arg, const ContextPtr & context) override;
    String getFormatFromFirstArgument() override;

private:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const std::string & compression_method_) const override;
    const char * getStorageTypeName() const override { return "File"; }
};

}
