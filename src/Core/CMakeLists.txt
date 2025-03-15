if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()

add_compile_options( -fbracket-depth=2048 )
