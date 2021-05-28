# Needed when using Apache Avro serialization format
option (ENABLE_AVRO "Enable Avro" ${ENABLE_LIBRARIES})

if (NOT ENABLE_AVRO)
    if (USE_INTERNAL_AVRO_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal avro library with ENABLE_AVRO=OFF")
    endif()
    return()
endif()

option (USE_INTERNAL_AVRO_LIBRARY
        "Set to FALSE to use system avro library instead of bundled" ON) # TODO: provide unbundled support

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/avro/lang/c++/CMakeLists.txt")
    if (USE_INTERNAL_AVRO_LIBRARY)
        message(WARNING "submodule contrib/avro is missing. to fix try run: \n git submodule update --init --recursive")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot find internal avro")
        set(USE_INTERNAL_AVRO_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_AVRO_LIBRARY 1)
endif()

if (NOT USE_INTERNAL_AVRO_LIBRARY)
   message (${RECONFIGURE_MESSAGE_LEVEL} "Using system avro library is not supported yet")
elseif(NOT MISSING_INTERNAL_AVRO_LIBRARY)
    include(cmake/find/snappy.cmake)
    set(AVROCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/avro/lang/c++/include")
    set(AVROCPP_LIBRARY avrocpp)
    set(USE_INTERNAL_AVRO_LIBRARY 1)
endif ()

if (AVROCPP_LIBRARY AND AVROCPP_INCLUDE_DIR)
    set(USE_AVRO 1)
endif()

message (STATUS "Using avro=${USE_AVRO}: ${AVROCPP_INCLUDE_DIR} : ${AVROCPP_LIBRARY}")
