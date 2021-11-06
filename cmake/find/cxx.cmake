option (USE_LIBCXX "Use libc++ and libc++abi instead of libstdc++" ${NOT_UNBUNDLED})

if (NOT USE_LIBCXX)
    if (USE_INTERNAL_LIBCXX_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal libcxx with USE_LIBCXX=OFF")
    endif()

    target_link_libraries(global-libs INTERFACE -l:libstdc++.a -l:libstdc++fs.a) # Always link these libraries as static
    target_link_libraries(global-libs INTERFACE ${EXCEPTION_HANDLING_LIBRARY})
    return()
endif()

set(USE_INTERNAL_LIBCXX_LIBRARY_DEFAULT ${NOT_UNBUNDLED})

option (USE_INTERNAL_LIBCXX_LIBRARY "Disable to use system libcxx and libcxxabi libraries instead of bundled"
    ${USE_INTERNAL_LIBCXX_LIBRARY_DEFAULT})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libcxx/CMakeLists.txt")
    if (USE_INTERNAL_LIBCXX_LIBRARY)
        message(WARNING "submodule contrib/libcxx is missing. to fix try run: \n git submodule update --init")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal libcxx")
        set(USE_INTERNAL_LIBCXX_LIBRARY 0)
    endif()
    set(USE_INTERNAL_LIBCXX_LIBRARY_DEFAULT 0)
    set(MISSING_INTERNAL_LIBCXX_LIBRARY 1)
endif()

set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG=0") # More checks in debug build.

if (NOT USE_INTERNAL_LIBCXX_LIBRARY)
    find_library (LIBCXX_LIBRARY c++)
    find_library (LIBCXXFS_LIBRARY c++fs)
    find_library (LIBCXXABI_LIBRARY c++abi)

    if(LIBCXX_LIBRARY AND LIBCXXABI_LIBRARY) # c++fs is now a part of the libc++
        set (HAVE_LIBCXX 1)
    else ()
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system libcxx")
    endif()

    if(NOT LIBCXXFS_LIBRARY)
        set(LIBCXXFS_LIBRARY ${LIBCXX_LIBRARY})
    endif()

    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")

    target_link_libraries(global-libs INTERFACE ${EXCEPTION_HANDLING_LIBRARY})
endif ()

if (NOT HAVE_LIBCXX AND NOT MISSING_INTERNAL_LIBCXX_LIBRARY)
    set (LIBCXX_LIBRARY cxx)
    set (LIBCXXABI_LIBRARY cxxabi)
    add_subdirectory(contrib/libcxxabi-cmake)
    add_subdirectory(contrib/libcxx-cmake)

    # Exception handling library is embedded into libcxxabi.

    set (HAVE_LIBCXX 1)
    set(USE_INTERNAL_LIBCXX_LIBRARY 1)
endif ()

if (HAVE_LIBCXX)
    target_link_libraries(global-libs INTERFACE ${LIBCXX_LIBRARY} ${LIBCXXABI_LIBRARY} ${LIBCXXFS_LIBRARY})

    message (STATUS "Using libcxx: ${LIBCXX_LIBRARY}")
    message (STATUS "Using libcxxfs: ${LIBCXXFS_LIBRARY}")
    message (STATUS "Using libcxxabi: ${LIBCXXABI_LIBRARY}")
else()
    target_link_libraries(global-libs INTERFACE -l:libstdc++.a -l:libstdc++fs.a) # Always link these libraries as static
    target_link_libraries(global-libs INTERFACE ${EXCEPTION_HANDLING_LIBRARY})
endif()
