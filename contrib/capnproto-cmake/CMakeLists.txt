option (ENABLE_CAPNP "Enable Cap'n Proto" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CAPNP)
    message(STATUS "Not using Cap'n Proto library")
    return()
endif()

set (CAPNPROTO_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/capnproto/c++/src")

set (CMAKE_CXX_STANDARD 17)

set (KJ_SRCS
    "${CAPNPROTO_SOURCE_DIR}/kj/array.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/common.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/debug.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/exception.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/io.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/memory.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/mutex.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/string.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/hash.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/table.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/thread.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/main.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/arena.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/test-helpers.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/units.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/encoding.c++"

    "${CAPNPROTO_SOURCE_DIR}/kj/refcount.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/string-tree.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/time.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/filesystem.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/filesystem-disk-unix.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/filesystem-disk-win32.c++"
    "${CAPNPROTO_SOURCE_DIR}/kj/parse/char.c++"
)

add_library(_kj ${KJ_SRCS})
target_include_directories(_kj SYSTEM PUBLIC ${CAPNPROTO_SOURCE_DIR})

set (CAPNP_SRCS
    "${CAPNPROTO_SOURCE_DIR}/capnp/c++.capnp.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/blob.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/arena.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/layout.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/list.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/any.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/message.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/schema.capnp.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/serialize.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/serialize-packed.c++"

    "${CAPNPROTO_SOURCE_DIR}/capnp/schema.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/stream.capnp.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/schema-loader.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/dynamic.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/stringify.c++"
)

add_library(_capnp ${CAPNP_SRCS})
set_target_properties(_capnp
                      PROPERTIES LINKER_LANGUAGE CXX
                      )
target_link_libraries(_capnp PUBLIC _kj)

set (CAPNPC_SRCS
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/type-id.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/error-reporter.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/lexer.capnp.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/lexer.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/grammar.capnp.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/parser.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/generics.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/node-translator.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/compiler/compiler.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/schema-parser.c++"
    "${CAPNPROTO_SOURCE_DIR}/capnp/serialize-text.c++"
)

add_library(_capnpc ${CAPNPC_SRCS})
target_link_libraries(_capnpc PUBLIC _capnp)

set (CAPNP_PRIVATE_CXX_FLAGS -fno-char8_t)

target_compile_options(_kj PRIVATE ${CAPNP_PRIVATE_CXX_FLAGS})
target_compile_options(_capnp PRIVATE ${CAPNP_PRIVATE_CXX_FLAGS})
target_compile_options(_capnpc PRIVATE ${CAPNP_PRIVATE_CXX_FLAGS})

add_library(ch_contrib::capnp ALIAS _capnpc)
