option(ENABLE_ANNOY "Enable Annoy index support" ${ENABLE_LIBRARIES})

# Annoy index should be disabled with undefined sanitizer. Because of memory storage optimizations
# (https://github.com/ClickHouse/annoy/blob/9d8a603a4cd252448589e84c9846f94368d5a289/src/annoylib.h#L442-L463)
# UBSan fails and leads to crash. Simmilar issue is already opened in Annoy repo
# https://github.com/spotify/annoy/issues/456
# Problem with aligment can lead to errors like
# (https://stackoverflow.com/questions/46790550/c-undefined-behavior-strict-aliasing-rule-or-incorrect-alignment)
# or will lead to crash on arm https://developer.arm.com/documentation/ka003038/latest
# This issues should be resolved before annoy became non-experimental (--> setting "allow_experimental_annoy_index")
if ((NOT ENABLE_ANNOY) OR (SANITIZE STREQUAL "undefined") OR (ARCH_AARCH64))
    message (STATUS "Not using annoy")
    return()
endif()

set(ANNOY_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/annoy")
set(ANNOY_SOURCE_DIR "${ANNOY_PROJECT_DIR}/src")

add_library(_annoy INTERFACE)
target_include_directories(_annoy SYSTEM INTERFACE ${ANNOY_SOURCE_DIR})

add_library(ch_contrib::annoy ALIAS _annoy)
target_compile_definitions(_annoy INTERFACE ENABLE_ANNOY)
target_compile_definitions(_annoy INTERFACE ANNOYLIB_MULTITHREADED_BUILD)
