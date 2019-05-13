if (NOT HAVE_AVX2)
    message (FATAL_ERROR "No AVX2 support")
endif ()

if(MAKE_STATIC_LIBRARIES)
    set(SIMDJSON_LIB_TYPE STATIC)
    MESSAGE(STATUS "Building static library ${SIMDJSON_LIBRARY}")
else()
    set(SIMDJSON_LIB_TYPE SHARED)
    MESSAGE(STATUS "Building dynamic library ${SIMDJSON_LIBRARY}")
endif()

set(SIMDJSON_SRC_DIR "${SIMDJSON_INCLUDE_DIR}/../src")
set(SIMDJSON_SRC
    ${SIMDJSON_SRC_DIR}/jsonioutil.cpp
    ${SIMDJSON_SRC_DIR}/jsonminifier.cpp
    ${SIMDJSON_SRC_DIR}/jsonparser.cpp
    ${SIMDJSON_SRC_DIR}/stage1_find_marks.cpp
    ${SIMDJSON_SRC_DIR}/stage2_build_tape.cpp
    ${SIMDJSON_SRC_DIR}/parsedjson.cpp
    ${SIMDJSON_SRC_DIR}/parsedjsoniterator.cpp
)

add_library(${SIMDJSON_LIBRARY} ${SIMDJSON_LIB_TYPE} ${SIMDJSON_SRC})
target_include_directories(${SIMDJSON_LIBRARY} PRIVATE "${SIMDJSON_INCLUDE_DIR}")
target_compile_options(${SIMDJSON_LIBRARY} PRIVATE -mavx2 -mbmi -mbmi2 -mpclmul)
