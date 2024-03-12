option(ENABLE_ION "Enable ion-c library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ION)
    message(STATUS "Not using ion-c library")
    return()
endif ()

set(IONC_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/ion-c")
set(IONC_SOURCE_DIR "${IONC_ROOT_DIR}/ionc")
set(IONC_INCLUDE_DIR "${IONC_SOURCE_DIR}/include")
set(IONC_DECNUMBER_SOURCE_DIR "${IONC_ROOT_DIR}/decNumber")
set(IONC_DECNUMBER_INCLUDE_DIR "${IONC_DECNUMBER_SOURCE_DIR}/include")

set(DECNUMBER_SOURCE_FILES
        "${IONC_DECNUMBER_SOURCE_DIR}/decContext.c"
        "${IONC_DECNUMBER_SOURCE_DIR}/decimal32.c"
        "${IONC_DECNUMBER_SOURCE_DIR}/decimal64.c"
        "${IONC_DECNUMBER_SOURCE_DIR}/decimal128.c"
        "${IONC_DECNUMBER_SOURCE_DIR}/decNumber.c"
        "${IONC_DECNUMBER_SOURCE_DIR}/decPacked.c"
        "${IONC_DECNUMBER_SOURCE_DIR}/decQuad.c"
)

add_library(_decNumber ${DECNUMBER_SOURCE_FILES})

target_compile_definitions(_decNumber PUBLIC DECNUMDIGITS=34)
target_include_directories(_decNumber SYSTEM PUBLIC ${IONC_DECNUMBER_INCLUDE_DIR})

#find_program(GIT_EXECUTABLE "git")
#add_custom_target(
#        version
#        ${CMAKE_COMMAND} -D SRC=${IONC_ROOT_DIR}/build_version.h.in
#        -D DST=${CMAKE_BINARY_DIR}/build_version.h
#        -D GIT_EXECUTABLE=${GIT_EXECUTABLE}
#        -P ${IONC_ROOT_DIR}/cmake/VersionHeader.cmake
#)

set(IONC_SOURCE_FILES
        "${IONC_SOURCE_DIR}/decQuadHelpers.c"
        "${IONC_SOURCE_DIR}/ion_allocation.c"
        "${IONC_SOURCE_DIR}/ion_binary.c"
        "${IONC_SOURCE_DIR}/ion_catalog.c"
        "${IONC_SOURCE_DIR}/ion_collection.c"
        "${IONC_SOURCE_DIR}/ion_debug.c"
        "${IONC_SOURCE_DIR}/ion_errors.c"
        "${IONC_SOURCE_DIR}/ion_helpers.c"
        "${IONC_SOURCE_DIR}/ion_index.c"
        "${IONC_SOURCE_DIR}/ion_initialize.c"
        "${IONC_SOURCE_DIR}/ion_int.c"
        "${IONC_SOURCE_DIR}/ion_reader_binary.c"
        "${IONC_SOURCE_DIR}/ion_reader.c"
        "${IONC_SOURCE_DIR}/ion_reader_text.c"
        "${IONC_SOURCE_DIR}/ion_scanner.c"
        "${IONC_SOURCE_DIR}/ion_stream.c"
        "${IONC_SOURCE_DIR}/ion_string.c"
        "${IONC_SOURCE_DIR}/ion_symbol_table.c"
        "${IONC_SOURCE_DIR}/ion_timestamp.c"
        "${IONC_SOURCE_DIR}/ion_writer_binary.c"
        "${IONC_SOURCE_DIR}/ion_writer.c"
        "${IONC_SOURCE_DIR}/ion_writer_text.c"
        "${IONC_SOURCE_DIR}/ion_float.c"
        "${IONC_SOURCE_DIR}/ion_decimal.c"
        "${IONC_SOURCE_DIR}/ion_extractor.c"
        #        "${IONC_SOURCE_DIR}/ion_version.c"
)

add_library(_ion ${IONC_SOURCE_FILES})

target_include_directories(_ion SYSTEM PUBLIC ${IONC_INCLUDE_DIR})

target_link_libraries(_ion PUBLIC _decNumber)

add_library(ch_contrib::ion ALIAS _ion)
