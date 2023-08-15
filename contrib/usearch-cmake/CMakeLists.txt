option(ENABLE_USEARCH "Enable USearch (Approximate Neighborhood Search, HNSW) support" ${ENABLE_LIBRARIES})

if (NOT ENABLE_USEARCH)
    message (STATUS "Not using usearch")
    return()
endif()

set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")
set(USEARCH_SOURCE_DIR "${USEARCH_PROJECT_DIR}/include")

add_library(_usearch INTERFACE)
target_include_directories(_usearch SYSTEM INTERFACE ${USEARCH_PROJECT_DIR}/fp16/include ${USEARCH_PROJECT_DIR}/robin-map/include ${USEARCH_PROJECT_DIR}/simsimd/include ${USEARCH_SOURCE_DIR})

add_library(ch_contrib::usearch ALIAS _usearch)
target_compile_definitions(_usearch INTERFACE ENABLE_USEARCH)