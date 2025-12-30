add_executable(lexer_test main.c)
target_include_directories(lexer_test PRIVATE ${CMAKE_SOURCE_DIR}/src/Parsers)
target_compile_options(lexer_test PRIVATE -std=c99 -Wno-vla -Wno-declaration-after-statement)
target_link_libraries(lexer_test PRIVATE lexer-c)
target_link_options(lexer_test PRIVATE ${COVERAGE_FLAGS})

if (ENABLE_LEXER_TEST)
    add_custom_command(TARGET lexer_test POST_BUILD COMMAND cp $<TARGET_FILE:lexer_test> "${CMAKE_BINARY_DIR}/programs/" VERBATIM)
endif ()
