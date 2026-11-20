add_library(_murmurhash
    src/MurmurHash2.cpp
    src/MurmurHash3.cpp
    include/MurmurHash2.h
    include/MurmurHash3.h)

target_include_directories(_murmurhash PUBLIC include)
add_library(ch_contrib::murmurhash ALIAS _murmurhash)

# With BTI, the jump table of the `switch` on the tail length (one indirect branch per hashed string) is slower
# than a few compare-and-branches.
if (USE_BRANCH_PROTECTION)
    target_compile_options(_murmurhash PRIVATE -fno-jump-tables)
endif ()
