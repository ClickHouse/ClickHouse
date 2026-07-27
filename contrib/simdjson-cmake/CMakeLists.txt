option (ENABLE_SIMDJSON "Use simdjson" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SIMDJSON)
    message(STATUS "Not using simdjson")
    return()
endif()

set(SIMDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include")
set(SIMDJSON_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/src")
set(SIMDJSON_SRC "${SIMDJSON_SRC_DIR}/simdjson.cpp")

add_library(_simdjson ${SIMDJSON_SRC})
target_include_directories(_simdjson SYSTEM PUBLIC "${SIMDJSON_INCLUDE_DIR}" PRIVATE "${SIMDJSON_SRC_DIR}")

# simdjson is using its own CPU dispatching and get confused if we enable AVX/AVX2 flags.
if(ARCH_AMD64)
    target_compile_options(_simdjson PRIVATE -mno-avx -mno-avx2)
endif()

# We allow fallback implementations because binaries are built with all
# supported instructions, so one implementation will be selected and fallback
# will be disabled.
#
# However, ClickHouse may be run on CPUs that do not support all of these
# instructions. Since fallback is disabled by default in such cases, this will
# result in an UNSUPPORTED_ARCHITECTURE error.
target_compile_options(_simdjson PRIVATE -DSIMDJSON_IMPLEMENTATION_FALLBACK=1)

add_library(ch_contrib::simdjson ALIAS _simdjson)
