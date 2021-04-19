# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
)


SRCS(
    ITableFunction.cpp
    ITableFunctionFileLike.cpp
    ITableFunctionXDBC.cpp
    TableFunctionDictionary.cpp
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
    parseColumnsListForTableFunction.cpp
    registerTableFunctions.cpp

)

END()
