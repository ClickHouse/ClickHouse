add_library(btrie
    src/btrie.c
    include/btrie.h
)

target_include_directories (btrie SYSTEM PUBLIC include)
