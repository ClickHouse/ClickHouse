option (USE_LIBCXX "Use libc++ and libc++abi instead of libstdc++" ON)

if (NOT USE_LIBCXX)
    target_link_libraries(global-libs INTERFACE -l:libstdc++.a -l:libstdc++fs.a) # Always link these libraries as static
    target_link_libraries(global-libs INTERFACE ${EXCEPTION_HANDLING_LIBRARY})
    return()
endif()

set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG=0") # More checks in debug build.

if (NOT HAVE_LIBCXX AND NOT MISSING_INTERNAL_LIBCXX_LIBRARY)
    set (LIBCXX_LIBRARY cxx)
    set (LIBCXXABI_LIBRARY cxxabi)
    add_subdirectory(contrib/libcxxabi-cmake)
    add_subdirectory(contrib/libcxx-cmake)

    # Exception handling library is embedded into libcxxabi.

    set (HAVE_LIBCXX 1)
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
