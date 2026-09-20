# Enable libcxx hardening, see https://libcxx.llvm.org/Hardening.html
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_EXTENSIVE")
else ()
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST")
endif ()

disable_dummy_launchers_if_needed()
add_subdirectory(contrib/libcxxabi-cmake)
add_subdirectory(contrib/libcxx-cmake)
enable_dummy_launchers_if_needed()

# Exception handling library is embedded into libcxxabi.

target_link_libraries(global-libs INTERFACE cxx cxxabi)
