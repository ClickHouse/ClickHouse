add_library(murmurhash
    src/murmurhash2.cpp
    include/murmurhash2.h)

target_include_directories (murmurhash PUBLIC include)
