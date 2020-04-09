#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB
{

class Context;

/* s3(source, format, structure) - creates a temporary storage for a file in S3
 */
class TableFunctionS3 : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "s3";
    std::string getName() const override
    {
        return name;
    }

private:
    StoragePtr getStorage(
        const String & source,
        const String & format,
        const ColumnsDescription & columns,
        Context & global_context,
        const std::string & table_name) const override;
};

}
