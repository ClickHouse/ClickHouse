# Build wrapper for the Jieba-CPP submodule (contrib/Jieba-CPP).
#
# The submodule ships its own standalone CMakeLists.txt for independent
# development; ClickHouse ignores that and builds the sources directly here,
# using its in-tree dependencies (abseil/darts-clone/zstd) instead of the
# dependency-discovery logic in the submodule's own build files.

set(JIEBA_DIR "${ClickHouse_SOURCE_DIR}/contrib/Jieba-CPP")

add_library(_jieba
    "${JIEBA_DIR}/src/jieba.cpp"
    "${JIEBA_DIR}/src/jieba_dict.cpp"
    "${JIEBA_DIR}/src/jieba_mp_seg.cpp"
    "${JIEBA_DIR}/src/jieba_hmm_seg.cpp")

# Public headers live in include/; the embedded data (dict_<le|be>.dat.zst via
# `#embed` and jieba_hmm_model.dat via a quoted `#include`) lives in data/. The
# sources reference the data with quoted includes, so make data/ an include dir
# and tell the compiler where `#embed` should resolve its operands.
target_include_directories(_jieba SYSTEM PUBLIC "${JIEBA_DIR}/include")
target_include_directories(_jieba PRIVATE "${JIEBA_DIR}/data")
target_compile_options(_jieba PRIVATE "--embed-dir=${JIEBA_DIR}/data")

target_link_libraries(_jieba PUBLIC
    ch_contrib::abseil_swiss_tables
    ch_contrib::darts-clone
    ch_contrib::zstd)

add_library(ch_contrib::jieba ALIAS _jieba)
