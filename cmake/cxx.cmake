# Enable libcxx hardening, see https://libcxx.llvm.org/Hardening.html
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_EXTENSIVE")
elseif (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
    set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST")
elseif (CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO")
    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST")
endif ()

disable_dummy_launchers_if_needed()
add_subdirectory(contrib/libcxxabi-cmake)
add_subdirectory(contrib/libcxx-cmake)
enable_dummy_launchers_if_needed()

# Exception handling library is embedded into libcxxabi.

target_link_libraries(global-libs INTERFACE cxx cxxabi)
