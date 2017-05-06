include_directories (BEFORE include)

add_library (lz4
    src/lz4.c
    src/lz4hc.c

    include/lz4/lz4.h
    include/lz4/lz4hc.h
    include/lz4/lz4opt.h)

