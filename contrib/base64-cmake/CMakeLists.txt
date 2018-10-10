SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/base64)

macro(add_glob cur_list)
    file(GLOB __tmp RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${ARGN})
    list(APPEND ${cur_list} ${__tmp})
endmacro()

macro(add_headers_and_sources prefix common_path)
    add_glob(${prefix}_headers RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.h ${common_path}/codec.c)
endmacro()


MACRO(SUBDIRLIST result curdir)
    FILE(GLOB children RELATIVE ${curdir} ${curdir}/*)
    SET(dirlist "")
    FOREACH(child ${children})
        IF(IS_DIRECTORY ${curdir}/${child})
            LIST(APPEND dirlist ${child})
        ENDIF()
    ENDFOREACH()
    SET(${result} ${dirlist})
ENDMACRO()

add_headers_and_sources(base64 ${LIBRARY_DIR}/include/)
add_headers_and_sources(base64 ${LIBRARY_DIR}/lib/)

SUBDIRLIST(SUBDIRS ${LIBRARY_DIR}/lib/arch/)
FOREACH(subdir ${SUBDIRS})
    add_headers_and_sources(base64 ${LIBRARY_DIR}/lib/arch/${subdir})
ENDFOREACH()

list(APPEND base64_sources ../base64/lib/lib.c ../base64/lib/codec_choose.c)

add_library (base64 ${LINK_MODE} ${base64_sources} config.h)

# todo fix
target_compile_definitions(base64 PUBLIC SSE41_CFLAGS=-msse4.1 SSE42_CFLAGS=-msse4.2)
target_include_directories(base64 PRIVATE ${LIBRARY_DIR}/include .)
