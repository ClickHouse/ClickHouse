option (SANITIZE "Enable sanitizer: address, memory, thread, undefined" "")

set (SAN_FLAGS "${SAN_FLAGS} -g -fno-omit-frame-pointer -DSANITIZER")

if (SANITIZE)
    if (SANITIZE STREQUAL "address")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libasan")
        endif ()
    elseif (SANITIZE STREQUAL "memory")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=memory")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=memory")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory")
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libmsan")
        endif ()
    elseif (SANITIZE STREQUAL "thread")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=thread")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=thread")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libtsan")
        endif ()
    elseif (SANITIZE STREQUAL "undefined")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS} -fsanitize=undefined -fno-sanitize-recover=all")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS} -fsanitize=undefined -fno-sanitize-recover=all")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
        if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libubsan")
        endif ()
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            # Devirtualization with multiple virtual inheritance confuses UBSan.
            # Example:
            #
            # ZooKeeperImpl.cpp:529:8: runtime error: member call on address 0x7f548d7ea490 which does not point to an object of type 'Request'
            # 0x7f548d7ea490: note: object has invalid vptr
            # 00 00 00 00  00 00 00 00 00 00 00 00  a8 a4 7e 8d 54 7f 00 00  00 00 00 00 00 00 00 00  00 00 00 00
            #
            # http://lists.llvm.org/pipermail/cfe-commits/Week-of-Mon-20161017/174163.html

            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-devirtualize")
        endif ()
    else ()
        message (FATAL_ERROR "Unknown sanitizer type: ${SANITIZE}")
    endif ()
endif()
