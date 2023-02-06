if (ENABLE_ODBC)
    if (NOT TARGET ch_contrib::unixodbc)
        message(FATAL_ERROR "Configuration error: unixodbc is not a target")
    endif()

    set (SRCS
        src/Binder.cpp
        src/ConnectionHandle.cpp
        src/Connector.cpp
        src/EnvironmentHandle.cpp
        src/Extractor.cpp
        src/ODBCException.cpp
        src/ODBCMetaColumn.cpp
        src/ODBCStatementImpl.cpp
        src/Parameter.cpp
        src/Preparator.cpp
        src/SessionImpl.cpp
        src/TypeInfo.cpp
        src/Unicode.cpp
        src/Utility.cpp
    )

    add_library (_poco_data_odbc ${SRCS})
    add_library (Poco::Data::ODBC ALIAS _poco_data_odbc)

    target_compile_options (_poco_data_odbc PRIVATE -Wno-unused-variable)
    target_include_directories (_poco_data_odbc SYSTEM PUBLIC "include")
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
