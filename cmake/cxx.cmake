if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    # Enable libcxx debug mode: https://releases.llvm.org/15.0.0/projects/libcxx/docs/DesignDocs/DebugMode.html
    # The docs say the debug mode violates complexity guarantees, so do this only for Debug builds.
    # set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_ENABLE_DEBUG_MODE=1")
    # ^^ Crashes the database upon startup, needs investigation.
    #    Besides that, the implementation looks like a poor man's MSAN specific to libcxx. Since CI tests MSAN
    #    anyways, we can keep the debug mode disabled.

    # Libcxx also provides extra assertions:
    # --> https://releases.llvm.org/15.0.0/projects/libcxx/docs/UsingLibcxx.html#assertions-mode
    # These look orthogonal to the debug mode but the debug mode enables them implicitly:
    # --> https://github.com/llvm/llvm-project/blob/release/15.x/libcxx/include/__assert#L29
    # They are cheap and straightforward, so enable them in debug builds:
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_ENABLE_ASSERTIONS=1")

    # TODO Once we upgrade to LLVM 18+, reconsider all of the above as they introduced "hardening modes":
    # https://libcxx.llvm.org/Hardening.html
endif ()

add_subdirectory(contrib/libcxxabi-cmake)
add_subdirectory(contrib/libcxx-cmake)

# Exception handling library is embedded into libcxxabi.

target_link_libraries(global-libs INTERFACE cxx cxxabi)
