option (ENABLE_ORC "Enable ORC" 1)

if(ENABLE_ORC)
option (USE_INTERNAL_ORC_LIBRARY "Set to FALSE to use system ORC instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/orc/c++/include/orc/OrcFile.hh")
   if(USE_INTERNAL_ORC_LIBRARY)
       message(WARNING "submodule contrib/orc is missing. to fix try run: \n git submodule update --init --recursive")
       set(USE_INTERNAL_ORC_LIBRARY 0)
   endif()
   set(MISSING_INTERNAL_ORC_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_ORC_LIBRARY)
    find_package(orc)
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
elseif(NOT MISSING_INTERNAL_ORC_LIBRARY AND ARROW_LIBRARY) # (LIBGSASL_LIBRARY AND LIBXML2_LIBRARY)
    set(ORC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/orc/c++/include")
    set(ORC_LIBRARY orc)
    set(USE_ORC 1)
else()
    set(USE_INTERNAL_ORC_LIBRARY 0)
endif()

endif()

message (STATUS "Using internal=${USE_INTERNAL_ORC_LIBRARY} orc=${USE_ORC}: ${ORC_INCLUDE_DIR} : ${ORC_LIBRARY}")
