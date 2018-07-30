add_library(murmurhash2
    src/murmurhash2.cpp
    include/murmurhash2.h)

target_include_directories (murmurhash2 PUBLIC include)
target_include_directories (murmurhash2 PUBLIC src)
