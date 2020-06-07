#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB
{

class Context;

/* url(source, format, structure) - creates a temporary storage from url
 */
class TableFunctionURL : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "url";
    std::string getName() const override
    {
        return name;
    }

private:
    StoragePtr getStorage(
        const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name, const String & compression_method) const override;
    const char * getStorageTypeName() const override { return "URL"; }
};

}
