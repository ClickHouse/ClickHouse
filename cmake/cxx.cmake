if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    # Enable libcxx debug mode: https://releases.llvm.org/15.0.0/projects/libcxx/docs/DesignDocs/DebugMode.html
    # The docs say the debug mode violates complexity guarantees, so do this only for Debug builds.
    # set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_ENABLE_DEBUG_MODE=1")
    # ^^ Crashes the database upon startup, needs investigation.
    #    Besides that, the implementation looks like a poor man's MSAN specific to libcxx. Since CI tests MSAN
    #    anyways, we can keep the debug mode disabled.

    # https://libcxx.llvm.org/Hardening.html
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_EXTENSIVE")
endif ()

disable_dummy_launchers_if_needed()
add_subdirectory(contrib/libcxxabi-cmake)
add_subdirectory(contrib/libcxx-cmake)
enable_dummy_launchers_if_needed()

# Exception handling library is embedded into libcxxabi.

target_link_libraries(global-libs INTERFACE cxx cxxabi)
