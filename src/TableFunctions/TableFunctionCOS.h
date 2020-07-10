#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionS3.h>


namespace DB
{

/* cosn(source, [access_key_id, secret_access_key,] format, structure) - creates a temporary storage for a file in COS 
 */
class TableFunctionCOS : public TableFunctionS3 
{
public:
    static constexpr auto name = "cosn";
    std::string getName() const override
    {
        return name;
    }

private:
    const char * getStorageTypeName() const override { return "COSN"; }
};

}

#endif
