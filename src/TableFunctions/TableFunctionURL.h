#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>
#include <Storages/StorageURL.h>
#include <IO/ReadWriteBufferFromHTTP.h>


namespace DB
{

class Context;

/* url(source, format[, structure, compression]) - creates a temporary storage from url.
 */
class TableFunctionURL : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "url";
    std::string getName() const override
    {
        return name;
    }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

protected:
    void parseArguments(const ASTPtr & ast, ContextPtr context) override;

private:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_) const override;

    const char * getStorageTypeName() const override { return "URL"; }

    String getFormatFromFirstArgument() override;

    StorageURL::Configuration configuration;
};

}
