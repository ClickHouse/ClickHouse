add_library (btrie
    src/btrie.c
    include/btrie.h
)

target_include_directories (btrie PUBLIC include)

if (ENABLE_TESTS)
    add_executable(test_btrie test/test_btrie.c)
    target_link_libraries(test_btrie btrie)
endif ()
