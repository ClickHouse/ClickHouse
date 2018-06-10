add_library(farmhash
	farmhash.cc
	farmhash.h)

if (MSVC)
    target_compile_definitions (farmhash PRIVATE FARMHASH_NO_BUILTIN_EXPECT=1)
endif ()

target_include_directories (farmhash PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
