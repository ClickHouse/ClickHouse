option (ENABLE_ORC "Enable ORC" ${ENABLE_LIBRARIES})

if(NOT ENABLE_ORC)
    if(USE_INTERNAL_ORC_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal ORC library with ENABLE_ORD=OFF")
    endif()
    return()
endif()

if (USE_INTERNAL_PARQUET_LIBRARY)
    option(USE_INTERNAL_ORC_LIBRARY "Set to FALSE to use system ORC instead of bundled (experimental set to OFF on your own risk)"
        ON)
elseif(USE_INTERNAL_ORC_LIBRARY)
       message (${RECONFIGURE_MESSAGE_LEVEL} "Currently internal ORC can be build only with bundled Parquet")
endif()

include(cmake/find/snappy.cmake)

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/orc/c++/include/orc/OrcFile.hh")
   if(USE_INTERNAL_ORC_LIBRARY)
       message(WARNING "submodule contrib/orc is missing. to fix try run: \n git submodule update --init --recursive")
       message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal ORC")
       set(USE_INTERNAL_ORC_LIBRARY 0)
   endif()
   set(MISSING_INTERNAL_ORC_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_ORC_LIBRARY)
    find_package(orc)
    if (NOT ORC_LIBRARY OR NOT ORC_INCLUDE_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system ORC")
    endif ()
endif ()

#if (USE_INTERNAL_ORC_LIBRARY)
#find_path(CYRUS_SASL_INCLUDE_DIR sasl/sasl.h)
#find_library(CYRUS_SASL_SHARED_LIB sasl2)
#if (NOT CYRUS_SASL_INCLUDE_DIR OR NOT CYRUS_SASL_SHARED_LIB)
#    set(USE_ORC 0)
#endif()
#endif()

if (ORC_LIBRARY AND ORC_INCLUDE_DIR)
    set(USE_ORC 1)
elseif(NOT MISSING_INTERNAL_ORC_LIBRARY AND ARROW_LIBRARY AND SNAPPY_LIBRARY) # (LIBGSASL_LIBRARY AND LIBXML2_LIBRARY)
    set(ORC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/orc/c++/include")
    set(ORC_LIBRARY orc)
    set(USE_ORC 1)
    set(USE_INTERNAL_ORC_LIBRARY 1)
else()
    message (${RECONFIGURE_MESSAGE_LEVEL}
             "Can't enable ORC support - missing dependencies. Missing internal orc=${MISSING_INTERNAL_ORC_LIBRARY}. "
             "arrow=${ARROW_LIBRARY} snappy=${SNAPPY_LIBRARY}")
    set(USE_INTERNAL_ORC_LIBRARY 0)
endif()

message (STATUS "Using internal=${USE_INTERNAL_ORC_LIBRARY} orc=${USE_ORC}: ${ORC_INCLUDE_DIR} : ${ORC_LIBRARY}")
