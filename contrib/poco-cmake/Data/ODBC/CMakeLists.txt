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
    else ()
        find_library(LIBRARY_POCO_DATA_ODBC PocoDataODBC)
        find_path(INCLUDE_POCO_DATA_ODBC Poco/Data/ODBC/ODBC.h)

        add_library (poco_data_odbc UNKNOWN IMPORTED)
        set_property (TARGET poco_data_odbc PROPERTY IMPORTED_LOCATION ${LIBRARY_POCO_DATA_ODBC})
        set_property (TARGET poco_data_odbc PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_POCO_DATA_ODBC})

        set (CMAKE_REQUIRED_LIBRARIES poco_data_odbc)
        check_cxx_source_compiles (
            "
            #include <Poco/Data/ODBC/Connector.h>

            int main() {
                Poco::Data::ODBC::Connector();
            }
            "
            EXTERNAL_POCO_DATA_ODBC_WORKS
        )

        if (NOT EXTERNAL_POCO_DATA_ODBC_WORKS)
            message (FATAL_ERROR "Poco::Data::ODBC is unusable: ${LIBRARY_POCO_DATA_ODBC} ${INCLUDE_POCO_DATA_ODBC}")
        endif ()
    endif ()

    message (STATUS "Using Poco::Data::ODBC")
else ()
    add_library (poco_data_odbc INTERFACE)

    message (STATUS "Not using Poco::Data::ODBC")
endif ()
