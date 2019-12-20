if (COMPILER_CLANG)
    option (USE_LIBCXX "Use libc++ and libc++abi instead of libstdc++" ON)
    option (USE_INTERNAL_LIBCXX_LIBRARY "Set to FALSE to use system libcxx and libcxxabi libraries instead of bundled" ${NOT_UNBUNDLED})
endif()

if (USE_LIBCXX)
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG=0") # More checks in debug build.

    if (NOT USE_INTERNAL_LIBCXX_LIBRARY)
        find_library (LIBCXX_LIBRARY c++)
        find_library (LIBCXXFS_LIBRARY c++fs)
        find_library (LIBCXXABI_LIBRARY c++abi)

        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")

        target_link_libraries(global-libs INTERFACE ${EXCEPTION_HANDLING_LIBRARY})
    else ()
        set (LIBCXX_LIBRARY cxx)
        set (LIBCXXABI_LIBRARY cxxabi)
        add_subdirectory(contrib/libcxxabi-cmake)
        add_subdirectory(contrib/libcxx-cmake)

        # Exception handling library is embedded into libcxxabi.
    endif ()

    target_link_libraries(global-libs INTERFACE ${LIBCXX_LIBRARY} ${LIBCXXABI_LIBRARY} ${LIBCXXFS_LIBRARY})

    set (HAVE_LIBCXX 1)

    message (STATUS "Using libcxx: ${LIBCXX_LIBRARY}")
    message (STATUS "Using libcxxfs: ${LIBCXXFS_LIBRARY}")
    message (STATUS "Using libcxxabi: ${LIBCXXABI_LIBRARY}")
else ()
    target_link_libraries(global-libs INTERFACE -l:libstdc++.a -l:libstdc++fs.a) # Always link these libraries as static
    target_link_libraries(global-libs INTERFACE ${EXCEPTION_HANDLING_LIBRARY})
endif ()
