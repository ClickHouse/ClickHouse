set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/MeCab/mecab")

# Source list mirrors `libmecab_la_SOURCES` in the upstream `src/Makefile.am`.
set(SRCS
    "${LIBRARY_DIR}/src/char_property.cpp"
    "${LIBRARY_DIR}/src/connector.cpp"
    "${LIBRARY_DIR}/src/context_id.cpp"
    "${LIBRARY_DIR}/src/dictionary.cpp"
    "${LIBRARY_DIR}/src/dictionary_compiler.cpp"
    "${LIBRARY_DIR}/src/dictionary_generator.cpp"
    "${LIBRARY_DIR}/src/dictionary_rewriter.cpp"
    "${LIBRARY_DIR}/src/eval.cpp"
    "${LIBRARY_DIR}/src/feature_index.cpp"
    "${LIBRARY_DIR}/src/iconv_utils.cpp"
    "${LIBRARY_DIR}/src/lbfgs.cpp"
    "${LIBRARY_DIR}/src/learner.cpp"
    "${LIBRARY_DIR}/src/learner_tagger.cpp"
    "${LIBRARY_DIR}/src/libmecab.cpp"
    "${LIBRARY_DIR}/src/nbest_generator.cpp"
    "${LIBRARY_DIR}/src/param.cpp"
    "${LIBRARY_DIR}/src/string_buffer.cpp"
    "${LIBRARY_DIR}/src/tagger.cpp"
    "${LIBRARY_DIR}/src/tokenizer.cpp"
    "${LIBRARY_DIR}/src/utils.cpp"
    "${LIBRARY_DIR}/src/viterbi.cpp"
    "${LIBRARY_DIR}/src/writer.cpp"
)

add_library(_mecab ${SRCS})
set_property(TARGET _mecab PROPERTY POSITION_INDEPENDENT_CODE ON)

# Build upstream MeCab unmodified; two flags accommodate modern C++/libc++ (no source edits):
#  * -Wno-register: old code uses the `register` keyword (an error in C++17+); contrib `-w` misses it.
#  * _LIBCPP_ENABLE_CXX17_REMOVED_UNARY_BINARY_FUNCTION: restores `std::binary_function` (used by
#    dictionary.cpp), which libc++ removed in C++17.
target_compile_options(_mecab PRIVATE -Wno-register)

# `config.h` lives here (not in the submodule); MeCab includes it under `HAVE_CONFIG_H`.
target_compile_definitions(_mecab PRIVATE HAVE_CONFIG_H _LIBCPP_ENABLE_CXX17_REMOVED_UNARY_BINARY_FUNCTION)
target_include_directories(_mecab PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

# The public API header `mecab.h` lives in `src/`.
target_include_directories(_mecab SYSTEM PUBLIC "${LIBRARY_DIR}/src")

add_library(ch_contrib::mecab ALIAS _mecab)
