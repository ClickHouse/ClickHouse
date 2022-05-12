# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

if (NOT EMSCRIPTEN)
    set (DEFAULT_LIBS "-nodefaultlibs")
endif ()

# We need builtins from Clang's RT even without libcxx - for ubsan+int128.
# See https://bugs.llvm.org/show_bug.cgi?id=16404
if (COMPILER_CLANG)
    execute_process (COMMAND ${CMAKE_CXX_COMPILER} --target=${CMAKE_CXX_COMPILER_TARGET} --print-libgcc-file-name --rtlib=compiler-rt OUTPUT_VARIABLE BUILTINS_LIBRARY OUTPUT_STRIP_TRAILING_WHITESPACE)

    if (NOT EXISTS "${BUILTINS_LIBRARY}")
        if (EMSCRIPTEN)
            # FIXME: explicitly setting builtins archives to link against for now
#            set (BUILTINS_LIBRARY
#                    "${CMAKE_SYSROOT}/lib/wasm64-emscripten/libc-debug.a
#                     ${CMAKE_SYSROOT}/lib/wasm64-emscripten/libcompiler_rt.a
#                     ${CMAKE_SYSROOT}/lib/wasm64-emscripten/libdlmalloc.a
#                     ${CMAKE_SYSROOT}/lib/wasm64-emscripten/libstubs-debug.a
#                     ${CMAKE_SYSROOT}/lib/wasm64-emscripten/libal.a")

        else ()
            set (BUILTINS_LIBRARY "-lgcc")
        endif ()
    endif ()
else ()
    set (BUILTINS_LIBRARY "-lgcc")
endif ()

if (OS_ANDROID)
    # pthread and rt are included in libc
    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -ldl")
elseif (USE_MUSL)
    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -static -lc")
elseif (EMSCRIPTEN)
    # enabling native js support for int64 and allowing heap growth at runtime
    # TODO: may increase overall available memory to 4 gb via -sMAXIMUM_MEMORY=4294967296
    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -sWASM_BIGINT -sALLOW_MEMORY_GROWTH -lm -lrt -lpthread -ldl")
else ()
    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# Unfortunately '-pthread' doesn't work with '-nodefaultlibs'.
# Just make sure we have pthreads at all.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if (NOT OS_ANDROID)
    if (NOT USE_MUSL)
        # Our compatibility layer doesn't build under Android, many errors in musl.
        add_subdirectory(base/glibc-compatibility)
    endif ()
    add_subdirectory(base/harmful)
endif ()

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

target_link_libraries(global-group INTERFACE
    -Wl,--start-group
    $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES>
    -Wl,--end-group
)

# FIXME: remove when all contribs will get custom cmake lists
install(
    TARGETS global-group global-libs
    EXPORT global
)
