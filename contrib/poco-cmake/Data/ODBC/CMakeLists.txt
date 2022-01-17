if (ENABLE_ODBC)
    if (NOT TARGET ch_contrib::unixodbc)
        message(FATAL_ERROR "Configuration error: unixodbc is not a target")
    endif()

    set (SRCS
        "${LIBRARY_DIR}/Data/ODBC/src/Binder.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/ConnectionHandle.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/Connector.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/EnvironmentHandle.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/Extractor.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/ODBCException.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/ODBCMetaColumn.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/ODBCStatementImpl.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/Parameter.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/Preparator.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/SessionImpl.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/TypeInfo.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/Unicode.cpp"
        "${LIBRARY_DIR}/Data/ODBC/src/Utility.cpp"
    )

    add_library (_poco_data_odbc ${SRCS})
    add_library (Poco::Data::ODBC ALIAS _poco_data_odbc)

    target_compile_options (_poco_data_odbc PRIVATE -Wno-unused-variable)
    target_include_directories (_poco_data_odbc SYSTEM PUBLIC "${LIBRARY_DIR}/Data/ODBC/include")
    target_link_libraries (_poco_data_odbc PUBLIC Poco::Data ch_contrib::unixodbc)

    message (STATUS "Using Poco::Data::ODBC")
else ()
    add_library (_poco_data_odbc INTERFACE)
    add_library (Poco::Data::ODBC ALIAS _poco_data_odbc)
    if (TARGET ch_contrib::unixodbc)
        target_link_libraries (_poco_data_odbc INTERFACE ch_contrib::unixodbc)
    endif()

    message (STATUS "Not using Poco::Data::ODBC")
endif ()
