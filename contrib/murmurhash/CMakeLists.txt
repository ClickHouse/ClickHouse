add_library(murmurhash
    src/MurmurHash2.cpp
    src/MurmurHash3.cpp
    include/MurmurHash2.h
    include/MurmurHash3.h)

target_include_directories (murmurhash PUBLIC include)
