option (USE_UNWIND "Enable libunwind (better stacktraces)" ${ENABLE_LIBRARIES})

if (USE_UNWIND)
    if (MAKE_STATIC_LIBRARIES)
        add_subdirectory(contrib/libunwind-cmake)
    else()
        add_subdirectory(${ClickHouse_SOURCE_DIR}/utils/local-engine/cmake/libunwind)
    endif()
    set (UNWIND_LIBRARIES unwind)
    set (EXCEPTION_HANDLING_LIBRARY ${UNWIND_LIBRARIES})

    message (STATUS "Using libunwind: ${UNWIND_LIBRARIES}")
else ()
    set (EXCEPTION_HANDLING_LIBRARY gcc_eh)
endif ()

message (STATUS "Using exception handler: ${EXCEPTION_HANDLING_LIBRARY}")
