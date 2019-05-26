set(BROTLI_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/brotli/c)
set(BROTLI_BINARY_DIR ${CMAKE_BINARY_DIR}/contrib/brotli/c)

set(SRCS
    ${BROTLI_SOURCE_DIR}/dec/bit_reader.c
    ${BROTLI_SOURCE_DIR}/dec/state.c
    ${BROTLI_SOURCE_DIR}/dec/huffman.c
    ${BROTLI_SOURCE_DIR}/dec/decode.c
    ${BROTLI_SOURCE_DIR}/enc/encode.c
    ${BROTLI_SOURCE_DIR}/enc/dictionary_hash.c
    ${BROTLI_SOURCE_DIR}/enc/cluster.c
    ${BROTLI_SOURCE_DIR}/enc/entropy_encode.c
    ${BROTLI_SOURCE_DIR}/enc/literal_cost.c
    ${BROTLI_SOURCE_DIR}/enc/compress_fragment_two_pass.c
    ${BROTLI_SOURCE_DIR}/enc/static_dict.c
    ${BROTLI_SOURCE_DIR}/enc/compress_fragment.c
    ${BROTLI_SOURCE_DIR}/enc/block_splitter.c
    ${BROTLI_SOURCE_DIR}/enc/backward_references_hq.c
    ${BROTLI_SOURCE_DIR}/enc/histogram.c
    ${BROTLI_SOURCE_DIR}/enc/brotli_bit_stream.c
    ${BROTLI_SOURCE_DIR}/enc/utf8_util.c
    ${BROTLI_SOURCE_DIR}/enc/encoder_dict.c
    ${BROTLI_SOURCE_DIR}/enc/backward_references.c
    ${BROTLI_SOURCE_DIR}/enc/bit_cost.c
    ${BROTLI_SOURCE_DIR}/enc/metablock.c
    ${BROTLI_SOURCE_DIR}/enc/memory.c
    ${BROTLI_SOURCE_DIR}/common/dictionary.c
    ${BROTLI_SOURCE_DIR}/common/transform.c
)

add_library(brotli ${SRCS})

target_include_directories(brotli PUBLIC ${BROTLI_SOURCE_DIR}/include)
