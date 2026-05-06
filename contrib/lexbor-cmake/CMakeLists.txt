set(LEXBOR_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/lexbor/source")

# Core module
set(LEXBOR_CORE_SOURCES
    ${LEXBOR_SOURCE_DIR}/lexbor/core/array.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/array_obj.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/avl.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/bst.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/bst_map.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/conv.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/diyfp.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/dobject.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/dtoa.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/hash.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/in.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/mem.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/mraw.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/plog.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/print.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/serialize.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/shs.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/str.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/strtod.c
    ${LEXBOR_SOURCE_DIR}/lexbor/core/utils.c
)

# POSIX port
set(LEXBOR_PORT_SOURCES
    ${LEXBOR_SOURCE_DIR}/lexbor/ports/posix/lexbor/core/fs.c
    ${LEXBOR_SOURCE_DIR}/lexbor/ports/posix/lexbor/core/memory.c
    ${LEXBOR_SOURCE_DIR}/lexbor/ports/posix/lexbor/core/perf.c
)

# DOM module
set(LEXBOR_DOM_SOURCES
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/collection.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/exception.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interface.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/attr.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/cdata_section.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/character_data.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/comment.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/document.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/document_fragment.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/document_type.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/element.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/event_target.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/node.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/processing_instruction.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/shadow_root.c
    ${LEXBOR_SOURCE_DIR}/lexbor/dom/interfaces/text.c
)

# Tag + NS modules
set(LEXBOR_TAG_SOURCES
    ${LEXBOR_SOURCE_DIR}/lexbor/tag/tag.c
    ${LEXBOR_SOURCE_DIR}/lexbor/ns/ns.c
)

# Encoding module
set(LEXBOR_ENCODING_SOURCES
    ${LEXBOR_SOURCE_DIR}/lexbor/encoding/decode.c
    ${LEXBOR_SOURCE_DIR}/lexbor/encoding/encode.c
    ${LEXBOR_SOURCE_DIR}/lexbor/encoding/encoding.c
    ${LEXBOR_SOURCE_DIR}/lexbor/encoding/multi_res.c
    ${LEXBOR_SOURCE_DIR}/lexbor/encoding/range_res.c
    ${LEXBOR_SOURCE_DIR}/lexbor/encoding/res.c
    ${LEXBOR_SOURCE_DIR}/lexbor/encoding/single_res.c
)

# HTML module
file(GLOB_RECURSE LEXBOR_HTML_SOURCES
    "${LEXBOR_SOURCE_DIR}/lexbor/html/*.c"
)

add_library(_lexbor
    ${LEXBOR_CORE_SOURCES}
    ${LEXBOR_PORT_SOURCES}
    ${LEXBOR_DOM_SOURCES}
    ${LEXBOR_TAG_SOURCES}
    ${LEXBOR_ENCODING_SOURCES}
    ${LEXBOR_HTML_SOURCES}
)

target_include_directories(_lexbor SYSTEM PUBLIC "${LEXBOR_SOURCE_DIR}")
target_compile_definitions(_lexbor PRIVATE
    LEXBOR_STATIC
    LEXBOR_WITHOUT_THREADS
    _POSIX_C_SOURCE=199309L
)
target_compile_options(_lexbor PRIVATE -std=c99 -w)

add_library(ch_contrib::lexbor ALIAS _lexbor)
