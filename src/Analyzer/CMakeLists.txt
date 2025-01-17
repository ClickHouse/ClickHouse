if (ENABLE_TESTS)
    add_subdirectory(tests)
    add_subdirectory(Passes/tests)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
