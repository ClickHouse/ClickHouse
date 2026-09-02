set(SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/re2")

set(RE2_SOURCES
    ${SRC_DIR}/re2/bitmap256.cc
    ${SRC_DIR}/re2/bitstate.cc
    ${SRC_DIR}/re2/compile.cc
    ${SRC_DIR}/re2/dfa.cc
    ${SRC_DIR}/re2/filtered_re2.cc
    ${SRC_DIR}/re2/mimics_pcre.cc
    ${SRC_DIR}/re2/nfa.cc
    ${SRC_DIR}/re2/onepass.cc
    ${SRC_DIR}/re2/parse.cc
    ${SRC_DIR}/re2/perl_groups.cc
    ${SRC_DIR}/re2/prefilter.cc
    ${SRC_DIR}/re2/prefilter_tree.cc
    ${SRC_DIR}/re2/prog.cc
    ${SRC_DIR}/re2/re2.cc
    ${SRC_DIR}/re2/regexp.cc
    ${SRC_DIR}/re2/set.cc
    ${SRC_DIR}/re2/simplify.cc
    ${SRC_DIR}/re2/tostring.cc
    ${SRC_DIR}/re2/unicode_casefold.cc
    ${SRC_DIR}/re2/unicode_groups.cc
    ${SRC_DIR}/util/rune.cc
    ${SRC_DIR}/util/strutil.cc
)

add_library(_re2 ${RE2_SOURCES})
target_include_directories(_re2 PUBLIC "${SRC_DIR}")
target_link_libraries(_re2 PRIVATE
    absl::absl_check
    absl::absl_log
    absl::base
    absl::core_headers
    absl::fixed_array
    absl::flags
    absl::flat_hash_map
    absl::flat_hash_set
    absl::hash
    absl::inlined_vector
    absl::optional
    absl::span
    absl::str_format
    absl::strings
    absl::synchronization)

add_library(ch_contrib::re2 ALIAS _re2)
