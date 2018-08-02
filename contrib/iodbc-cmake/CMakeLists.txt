set(IODBC_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/iodbc)
set(IODBC_BINARY_DIR ${CMAKE_BINARY_DIR}/contrib/iodbc)

set(SRCS
${IODBC_SOURCE_DIR}/iodbc/catalog.c
${IODBC_SOURCE_DIR}/iodbc/connect.c
${IODBC_SOURCE_DIR}/iodbc/dlproc.c
${IODBC_SOURCE_DIR}/iodbc/execute.c
${IODBC_SOURCE_DIR}/iodbc/fetch.c
${IODBC_SOURCE_DIR}/iodbc/hdbc.c
${IODBC_SOURCE_DIR}/iodbc/henv.c
${IODBC_SOURCE_DIR}/iodbc/herr.c
${IODBC_SOURCE_DIR}/iodbc/hstmt.c
${IODBC_SOURCE_DIR}/iodbc/info.c
${IODBC_SOURCE_DIR}/iodbc/main.c
${IODBC_SOURCE_DIR}/iodbc/misc.c
${IODBC_SOURCE_DIR}/iodbc/odbc3.c
${IODBC_SOURCE_DIR}/iodbc/prepare.c
${IODBC_SOURCE_DIR}/iodbc/result.c
${IODBC_SOURCE_DIR}/iodbc/trace/AllocConnect.c
${IODBC_SOURCE_DIR}/iodbc/trace/AllocEnv.c
${IODBC_SOURCE_DIR}/iodbc/trace/AllocHandle.c
${IODBC_SOURCE_DIR}/iodbc/trace/AllocStmt.c
${IODBC_SOURCE_DIR}/iodbc/trace/BindCol.c
${IODBC_SOURCE_DIR}/iodbc/trace/BindParameter.c
${IODBC_SOURCE_DIR}/iodbc/trace/BrowseConnect.c
${IODBC_SOURCE_DIR}/iodbc/trace/BulkOperations.c
${IODBC_SOURCE_DIR}/iodbc/trace/Cancel.c
${IODBC_SOURCE_DIR}/iodbc/trace/CloseCursor.c
${IODBC_SOURCE_DIR}/iodbc/trace/ColAttribute.c
${IODBC_SOURCE_DIR}/iodbc/trace/ColumnPrivileges.c
${IODBC_SOURCE_DIR}/iodbc/trace/Columns.c
${IODBC_SOURCE_DIR}/iodbc/trace/Connect.c
${IODBC_SOURCE_DIR}/iodbc/trace/CopyDesc.c
${IODBC_SOURCE_DIR}/iodbc/trace/DataSources.c
${IODBC_SOURCE_DIR}/iodbc/trace/DescribeCol.c
${IODBC_SOURCE_DIR}/iodbc/trace/DescribeParam.c
${IODBC_SOURCE_DIR}/iodbc/trace/Disconnect.c
${IODBC_SOURCE_DIR}/iodbc/trace/DriverConnect.c
${IODBC_SOURCE_DIR}/iodbc/trace/Drivers.c
${IODBC_SOURCE_DIR}/iodbc/trace/EndTran.c
${IODBC_SOURCE_DIR}/iodbc/trace/Error.c
${IODBC_SOURCE_DIR}/iodbc/trace/ExecDirect.c
${IODBC_SOURCE_DIR}/iodbc/trace/Execute.c
${IODBC_SOURCE_DIR}/iodbc/trace/ExtendedFetch.c
${IODBC_SOURCE_DIR}/iodbc/trace/Fetch.c
${IODBC_SOURCE_DIR}/iodbc/trace/FetchScroll.c
${IODBC_SOURCE_DIR}/iodbc/trace/ForeignKeys.c
${IODBC_SOURCE_DIR}/iodbc/trace/FreeConnect.c
${IODBC_SOURCE_DIR}/iodbc/trace/FreeEnv.c
${IODBC_SOURCE_DIR}/iodbc/trace/FreeHandle.c
${IODBC_SOURCE_DIR}/iodbc/trace/FreeStmt.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetConnectAttr.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetConnectOption.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetCursorName.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetData.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetDescField.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetDescRec.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetDiagField.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetDiagRec.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetEnvAttr.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetFunctions.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetStmtAttr.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetStmtOption.c
${IODBC_SOURCE_DIR}/iodbc/trace/GetTypeInfo.c
${IODBC_SOURCE_DIR}/iodbc/trace/Info.c
${IODBC_SOURCE_DIR}/iodbc/trace/MoreResults.c
${IODBC_SOURCE_DIR}/iodbc/trace/NativeSql.c
${IODBC_SOURCE_DIR}/iodbc/trace/NumParams.c
${IODBC_SOURCE_DIR}/iodbc/trace/NumResultCols.c
${IODBC_SOURCE_DIR}/iodbc/trace/ParamData.c
${IODBC_SOURCE_DIR}/iodbc/trace/ParamOptions.c
${IODBC_SOURCE_DIR}/iodbc/trace/Prepare.c
${IODBC_SOURCE_DIR}/iodbc/trace/PrimaryKeys.c
${IODBC_SOURCE_DIR}/iodbc/trace/ProcedureColumns.c
${IODBC_SOURCE_DIR}/iodbc/trace/Procedures.c
${IODBC_SOURCE_DIR}/iodbc/trace/PutData.c
${IODBC_SOURCE_DIR}/iodbc/trace/RowCount.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetConnectAttr.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetConnectOption.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetCursorName.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetDescField.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetDescRec.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetEnvAttr.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetPos.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetScrollOptions.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetStmtAttr.c
${IODBC_SOURCE_DIR}/iodbc/trace/SetStmtOption.c
${IODBC_SOURCE_DIR}/iodbc/trace/SpecialColumns.c
${IODBC_SOURCE_DIR}/iodbc/trace/Statistics.c
${IODBC_SOURCE_DIR}/iodbc/trace/TablePrivileges.c
${IODBC_SOURCE_DIR}/iodbc/trace/Tables.c
${IODBC_SOURCE_DIR}/iodbc/trace/trace.c
${IODBC_SOURCE_DIR}/iodbc/trace/Transact.c
)

add_library(iodbc STATIC ${SRCS})

target_include_directories(iodbc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/linux_x86_64/include)
target_include_directories(iodbc PUBLIC ${IODBC_SOURCE_DIR}/include)
target_include_directories(iodbc PUBLIC ${IODBC_SOURCE_DIR}/iodbc)
target_include_directories(iodbc PUBLIC ${IODBC_SOURCE_DIR}/iodbcinst)

# The library looks like trash.
target_compile_options(iodbc PRIVATE
    -Wno-self-assign -Wno-pointer-sign -Wno-sometimes-uninitialized -Wno-address -Wno-stringop-overflow -Wno-strict-overflow -Wno-implicit-function-declaration)
