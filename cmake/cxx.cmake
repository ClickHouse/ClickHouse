if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    # Enable libcxx debug mode:
    # --> https://releases.llvm.org/15.0.0/projects/libcxx/docs/DesignDocs/DebugMode.html
    # The docs say the debug mode violates complexity guarantees, so do this only for Debug builds.
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_ENABLE_DEBUG_MODE=1")

    # Also, enable randomization of unspecified behavior, e.g. the order of equal elements in std::sort.
    # "Randomization" means that Debug and Release builds use different seeds.
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG_RANDOMIZE_UNSPECIFIED_STABILITY_SEED=12345678")

    # Note: Libcxx also has provides extra assertions:
    # --> https://releases.llvm.org/15.0.0/projects/libcxx/docs/UsingLibcxx.html#assertions-mode
    # These look orthogonal to the debug mode but the debug mode imlicitly enables them:
    # --> https://github.com/llvm/llvm-project/blob/release/15.x/libcxx/include/__assert#L29

    # TODO Once we upgrade to LLVM 18+, reconsider all of the above as they introduced "hardening modes":
    # https://libcxx.llvm.org/Hardening.html
endif ()

add_subdirectory(contrib/libcxxabi-cmake)
add_subdirectory(contrib/libcxx-cmake)

# Exception handling library is embedded into libcxxabi.

target_link_libraries(global-libs INTERFACE cxx cxxabi)
