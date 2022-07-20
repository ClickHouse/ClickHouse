option(ENABLE_ANNOY "Enable Annoy index support" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ANNOY)
    message (STATUS "Not using annoy")
    return()
endif()

set(ANNOY_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/annoy")
set(ANNOY_SOURCE_DIR "${ANNOY_PROJECT_DIR}/src")

add_library(_annoy INTERFACE)
target_include_directories(_annoy SYSTEM INTERFACE ${ANNOY_SOURCE_DIR})

add_library(ch_contrib::annoy ALIAS _annoy)
target_compile_definitions(_annoy INTERFACE ENABLE_ANNOY)
