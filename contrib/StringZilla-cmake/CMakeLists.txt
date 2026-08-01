set(STRINGZILLA_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/StringZilla")

SET(STRINGZILLA_SOURCES
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/runtime.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/compare.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/memory.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/hash.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/find.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/sort.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/intersect.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_norm.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_runes.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_tokens.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_wordbreaks.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_graphemes.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_sentences.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_linebreaks.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_uncased_fold.c"
    "${STRINGZILLA_SOURCE_DIR}/c/stringzilla/utf8_uncased.c"
)
file(GLOB_RECURSE STRINGZILLA_HEADERS "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/*.h" "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/*.hpp")

add_library(_stringzilla ${STRINGZILLA_SOURCES} ${STRINGZILLA_HEADERS})
add_library(ch_contrib::stringzilla ALIAS _stringzilla)
target_include_directories(_stringzilla SYSTEM PUBLIC "${STRINGZILLA_SOURCE_DIR}/include")

if (ARCH_AMD64)
    target_compile_definitions(
            _stringzilla PRIVATE
            "SZ_USE_WESTMERE=1"
            "SZ_USE_GOLDMONT=1"
            "SZ_USE_HASWELL=1"
            "SZ_USE_SKYLAKE=1"
            "SZ_USE_ICELAKE=1"
            "SZ_USE_NEON=0"
            "SZ_USE_NEONAES=0"
            "SZ_USE_NEONSHA=0"
            "SZ_USE_SVE=0"
            "SZ_USE_SVE2=0"
            "SZ_USE_SVE2AES=0"
    )
elseif (ARCH_AARCH64)
    target_compile_definitions(
            _stringzilla PRIVATE
            "SZ_USE_WESTMERE=0"
            "SZ_USE_GOLDMONT=0"
            "SZ_USE_HASWELL=0"
            "SZ_USE_SKYLAKE=0"
            "SZ_USE_ICELAKE=0"
            "SZ_USE_NEON=1"
            "SZ_USE_NEONAES=1"
            "SZ_USE_NEONSHA=1"
            "SZ_USE_SVE=1"
            "SZ_USE_SVE2=1"
            "SZ_USE_SVE2AES=1"
    )
endif ()

# We need SZ_DYNAMIC_DISPATCH=1 to be public because otherwise the headers will redefine the API (sz_find for example)
# again and avoid dynamic dispatch.
target_compile_definitions(_stringzilla PUBLIC "SZ_DYNAMIC_DISPATCH=1")
target_compile_definitions(_stringzilla PRIVATE "SZ_AVOID_LIBC=0")
target_compile_definitions(_stringzilla PRIVATE "SZ_DEBUG=0")
