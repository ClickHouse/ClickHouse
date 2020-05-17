LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    ITableFunction.cpp
    ITableFunctionFileLike.cpp
    ITableFunctionXDBC.cpp
    parseColumnsListForTableFunction.cpp
    registerTableFunctions.cpp
    TableFunctionFactory.cpp
    TableFunctionFile.cpp
    TableFunctionGenerateRandom.cpp
    TableFunctionInput.cpp
    TableFunctionMerge.cpp
    TableFunctionMySQL.cpp
    TableFunctionNumbers.cpp
    TableFunctionRemote.cpp
    TableFunctionURL.cpp
    TableFunctionValues.cpp
    TableFunctionZeros.cpp
)

END()
