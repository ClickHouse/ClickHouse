# Uses MAKE_STATIC_LIBRARIES


set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package (Threads)

include (cmake/test_compiler.cmake)
include (cmake/arch.cmake)

if (OS_LINUX AND COMPILER_CLANG)
    set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS}")

    option (USE_LIBCXX "Use libc++ and libc++abi instead of libstdc++ (only make sense on Linux with Clang)" ${HAVE_LIBCXX})
    set (LIBCXX_PATH "" CACHE STRING "Use custom path for libc++. It should be used for MSan.")

    if (USE_LIBCXX)
        set (CMAKE_CXX_FLAGS             "${CMAKE_CXX_FLAGS} -stdlib=libc++") # Ok for clang6, for older can cause 'not used option' warning
        set (CMAKE_CXX_FLAGS_DEBUG       "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG=0") # More checks in debug build.
        if (MAKE_STATIC_LIBRARIES)
            execute_process (COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=libclang_rt.builtins-${CMAKE_SYSTEM_PROCESSOR}.a OUTPUT_VARIABLE BUILTINS_LIB_PATH OUTPUT_STRIP_TRAILING_WHITESPACE)
            link_libraries (-nodefaultlibs -Wl,-Bstatic -stdlib=libc++ c++ c++abi gcc_eh ${BUILTINS_LIB_PATH} rt -Wl,-Bdynamic dl pthread m c)
        else ()
            link_libraries (-stdlib=libc++ c++ c++abi)
        endif ()

        if (LIBCXX_PATH)
#            include_directories (SYSTEM BEFORE "${LIBCXX_PATH}/include" "${LIBCXX_PATH}/include/c++/v1")
            link_directories ("${LIBCXX_PATH}/lib")
        endif ()
    endif ()
endif ()

if (USE_LIBCXX)
    set (STATIC_STDLIB_FLAGS "")
else ()
    set (STATIC_STDLIB_FLAGS "-static-libgcc -static-libstdc++")
endif ()

if (MAKE_STATIC_LIBRARIES AND NOT APPLE AND NOT (COMPILER_CLANG AND OS_FREEBSD))
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${STATIC_STDLIB_FLAGS}")

    # Along with executables, we also build example of shared library for "library dictionary source"; and it also should be self-contained.
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${STATIC_STDLIB_FLAGS}")
endif ()

if (USE_STATIC_LIBRARIES AND HAVE_NO_PIE)
    set (CMAKE_CXX_FLAGS                 "${CMAKE_CXX_FLAGS} ${FLAG_NO_PIE}")
    set (CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} ${FLAG_NO_PIE}")
endif ()
