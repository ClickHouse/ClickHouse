# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

CFLAGS(-g0)

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
    TableFunctionNull.cpp
    TableFunctionNumbers.cpp
    TableFunctionRemote.cpp
    TableFunctionURL.cpp
    TableFunctionValues.cpp
    TableFunctionView.cpp
    TableFunctionZeros.cpp

)

END()
