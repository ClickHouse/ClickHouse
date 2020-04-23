if (ENABLE_ODBC)
    if (USE_INTERNAL_POCO_LIBRARY)
        set (SRCS
            ${LIBRARY_DIR}/Data/ODBC/src/Binder.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/ConnectionHandle.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/Connector.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/EnvironmentHandle.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/Extractor.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/ODBCException.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/ODBCMetaColumn.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/ODBCStatementImpl.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/Parameter.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/Preparator.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/SessionImpl.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/TypeInfo.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/Unicode.cpp
            ${LIBRARY_DIR}/Data/ODBC/src/Utility.cpp
        )

        add_library (poco_data_odbc ${SRCS})

        target_compile_options (poco_data_odbc PUBLIC -Wno-unused-variable)
        target_include_directories (poco_data_odbc PUBLIC ${LIBRARY_DIR}/Data/ODBC/include)
        target_link_libraries (poco_data_odbc PUBLIC poco_data unixodbc)
    endif ()
endif ()
