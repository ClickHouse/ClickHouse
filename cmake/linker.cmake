option (LINKER_NAME "Linker name or full path")

set (LLD_NAMES "ld.lld" "lld")

if (CMAKE_CXX_COMPILER)
    string(REGEX MATCH "-?[0-9]+(.[0-9]+)?$" COMPILER_POSTFIX ${CMAKE_CXX_COMPILER})
    list(APPEND LLD_NAMES "lld${COMPILER_POSTFIX}")
endif ()

find_program (LLD_PATH NAMES ${LLD_NAMES})
find_program (GOLD_PATH NAMES "ld.gold" "gold")

if (NOT LINKER_NAME)
    if (LLD_PATH)
        set (LINKER_NAME "lld")
        set (CMAKE_LINKER ${LLD_PATH})
    elseif (GOLD_PATH)
        set (LINKER_NAME "gold")
        set (CMAKE_LINKER ${GOLD_PATH})
    endif ()
endif ()

if (LINKER_NAME)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    list (APPEND CMAKE_REQUIRED_LINK_OPTIONS "-fuse-ld=${LINKER_NAME}")

    message(STATUS "Using linker: ${LINKER_NAME} (selected from: LLD_PATH=${LLD_PATH} GOLD_PATH=${GOLD_PATH})")
endif ()
