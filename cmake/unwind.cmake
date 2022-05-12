option (USE_UNWIND "Enable libunwind (better stacktraces)" ${ENABLE_LIBRARIES})

if (EMSCRIPTEN)
    # TODO: there is a libunwind wasm port, could potentially adopt it to ClickHouse: https://github.com/emscripten-core/emscripten/tree/main/system/lib/libunwind
    set (USE_UNWIND OFF)
endif()

if (USE_UNWIND)
    add_subdirectory(contrib/libunwind-cmake)
    set (UNWIND_LIBRARIES unwind)
    set (EXCEPTION_HANDLING_LIBRARY ${UNWIND_LIBRARIES})

    message (STATUS "Using libunwind: ${UNWIND_LIBRARIES}")
elseif (EMSCRIPTEN)
# TODO: ignoring exceptions in Emscripten, because its 64-bit mode is incompatible with exception handling
else ()
    set (EXCEPTION_HANDLING_LIBRARY gcc_eh)
endif ()

message (STATUS "Using exception handler: ${EXCEPTION_HANDLING_LIBRARY}")
