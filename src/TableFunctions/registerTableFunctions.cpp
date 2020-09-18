#include "registerTableFunctions.h"
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
void registerTableFunctions()
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionMerge(factory);
    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionNull(factory);
    registerTableFunctionZeros(factory);
    registerTableFunctionFile(factory);
    registerTableFunctionURL(factory);
    registerTableFunctionValues(factory);
    registerTableFunctionInput(factory);
    registerTableFunctionGenerate(factory);

#if USE_AWS_S3
    registerTableFunctionS3(factory);
    registerTableFunctionCOS(factory);
#endif

#if USE_HDFS
    registerTableFunctionHDFS(factory);
#endif

    registerTableFunctionODBC(factory);
    registerTableFunctionJDBC(factory);

    registerTableFunctionView(factory);

#if USE_MYSQL
    registerTableFunctionMySQL(factory);
#endif
}

}
