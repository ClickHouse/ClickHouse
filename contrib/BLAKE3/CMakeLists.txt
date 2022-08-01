if((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "toolchain-aarch64"))
	message(STATUS "BLAKE3 is currently not supported on aarch64-apple-darwin platform")
	set(ENABLE_BLAKE3 0)
	set(USE_BLAKE3 0)
endif()

include(build_rust_lib.cmake)

build_cargo(blake3 ${CMAKE_CURRENT_SOURCE_DIR})
