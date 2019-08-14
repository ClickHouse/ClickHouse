option (USE_UNWIND "Enable libunwind (better stacktraces)" ON)

if (CMAKE_SYSTEM MATCHES "Linux" AND NOT ARCH_ARM AND NOT ARCH_32)
    option (USE_INTERNAL_UNWIND_LIBRARY "Set to FALSE to use system unwind library instead of bundled" ${NOT_UNBUNDLED})
else ()
    option (USE_INTERNAL_UNWIND_LIBRARY "Set to FALSE to use system unwind library instead of bundled" OFF)
endif ()

if (USE_UNWIND)
    if (NOT USE_INTERNAL_UNWIND_LIBRARY)
        find_library (UNWIND_LIBRARY unwind)
    else ()
        option (USE_INTERNAL_UNWIND_LIBRARY_FOR_EXCEPTION_HANDLING "Use internal unwind library for exception handling" ${USE_STATIC_LIBRARIES})

        set (UNWIND_LIBRARY unwind)
        add_subdirectory(contrib/libunwind-cmake)
    endif ()

    # If we don't use built-in libc++abi, then we have to link directly with an exception handling library
    if (NOT USE_LIBCXX OR NOT USE_INTERNAL_LIBCXX_LIBRARY)
        if (USE_INTERNAL_UNWIND_LIBRARY_FOR_EXCEPTION_HANDLING)
            set (EXCEPTION_HANDLING_LIBRARY ${UNWIND_LIBRARY})
        else ()
            set (EXCEPTION_HANDLING_LIBRARY -lgcc_eh)
        endif ()
    endif ()

    link_libraries(${EXCEPTION_HANDLING_LIBRARY})

    message (STATUS "Using libunwind: ${UNWIND_LIBRARY}")
    message (STATUS "Using exception handler: ${EXCEPTION_HANDLING_LIBRARY}")
endif ()
