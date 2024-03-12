option (ENABLE_CBOR "Enable cbor library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CBOR)
    message(STATUS "Not using cbor")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/cbor")

set(SRCS
    "${LIBRARY_DIR}/src/encoder.cpp"
    "${LIBRARY_DIR}/src/decoder.cpp"
    "${LIBRARY_DIR}/src/input.cpp"
    "${LIBRARY_DIR}/src/listener_debug.cpp"
    "${LIBRARY_DIR}/src/output_dynamic.cpp"
    "${LIBRARY_DIR}/src/output_static.cpp"
)


add_library(_cbor INTERFACE ${SRCS})
target_include_directories(_cbor SYSTEM BEFORE INTERFACE "${LIBRARY_DIR}/src")
add_library(ch_contrib::cbor ALIAS _cbor)

