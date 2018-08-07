add_library(murmurhash
    src/murmurhash2.cpp
    src/murmurhash3.cpp
    include/murmurhash2.h
    include/murmurhash3.h)

target_include_directories (murmurhash PUBLIC include)
