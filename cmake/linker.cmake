option (LINKER_NAME "Linker name or full path")

find_program (LLD_PATH NAMES "ld.lld" "lld")
find_program (GOLD_PATH NAMES "ld.gold" "gold")

if (NOT LINKER_NAME)
    if (LLD_PATH)
        set (LINKER_NAME "lld")
    elseif (GOLD_PATH)
        set (LINKER_NAME "gold")
    endif ()
endif ()

if (LINKER_NAME)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")

    message(STATUS "Using custom linker by name: ${LINKER_NAME}")
endif ()
