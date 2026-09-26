set(JSON_DIR ${ClickHouse_SOURCE_DIR}/contrib/nlohmann-json)

# `nlohmann/json.hpp` reads the decimal point and the thousands separator from `localeconv`.
# ClickHouse always runs in the `C` locale, and `localeconv` is on the list of harmful functions
# (see `base/harmful/harmful.c`), so calling it traps in debug and sanitizer builds. The library is
# used from an upstream submodule that we cannot patch in place, so generate a patched copy of the
# amalgamated header with the locale lookup replaced by the `C` locale constants. This is the same
# patch that the bundled copy of the library inside the Azure SDK carries.

set(JSON_PATCHED_DIR "${CMAKE_CURRENT_BINARY_DIR}/patched_include")

file(READ "${JSON_DIR}/single_include/nlohmann/json.hpp" JSON_SOURCE)

set(JSON_LEXER_ORIGINAL [[        const auto* loc = localeconv();
        JSON_ASSERT(loc != nullptr);
        return (loc->decimal_point == nullptr) ? '.' : *(loc->decimal_point);]])

set(JSON_LEXER_PATCHED [[        // Patched for ClickHouse: the `C` locale is always used, and `localeconv` is harmful.
        return '.';]])

set(JSON_SERIALIZER_ORIGINAL [[        , loc(std::localeconv())
        , thousands_sep(loc->thousands_sep == nullptr ? '\0' : std::char_traits<char>::to_char_type(* (loc->thousands_sep)))
        , decimal_point(loc->decimal_point == nullptr ? '\0' : std::char_traits<char>::to_char_type(* (loc->decimal_point)))]])

set(JSON_SERIALIZER_PATCHED [[        // Patched for ClickHouse: the `C` locale is always used, and `localeconv` is harmful.
        , loc(nullptr)
        , thousands_sep('\0')
        , decimal_point('.')]])

foreach (JSON_FRAGMENT JSON_LEXER_ORIGINAL JSON_SERIALIZER_ORIGINAL)
    string(FIND "${JSON_SOURCE}" "${${JSON_FRAGMENT}}" JSON_FRAGMENT_POSITION)
    if (JSON_FRAGMENT_POSITION EQUAL -1)
        message(FATAL_ERROR "Cannot patch `localeconv` out of `nlohmann/json.hpp`: ${JSON_FRAGMENT} not found. The library was probably updated; adjust the patch in ${CMAKE_CURRENT_LIST_FILE}.")
    endif ()
endforeach ()

string(REPLACE "${JSON_LEXER_ORIGINAL}" "${JSON_LEXER_PATCHED}" JSON_SOURCE "${JSON_SOURCE}")
string(REPLACE "${JSON_SERIALIZER_ORIGINAL}" "${JSON_SERIALIZER_PATCHED}" JSON_SOURCE "${JSON_SOURCE}")

# Write through a temporary file, so that the header is not touched when nothing changed.
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/json.hpp.tmp" "${JSON_SOURCE}")
configure_file("${CMAKE_CURRENT_BINARY_DIR}/json.hpp.tmp" "${JSON_PATCHED_DIR}/nlohmann/json.hpp" COPYONLY)

add_library(_nlohmann_json INTERFACE)
# The patched directory goes first, so that its `json.hpp` shadows the original one, while the rest
# of the headers (`json_fwd.hpp`) are still taken from the library.
set_property(
    TARGET _nlohmann_json
    APPEND
    PROPERTY INTERFACE_INCLUDE_DIRECTORIES
             ${JSON_PATCHED_DIR}
             ${JSON_DIR}/single_include)
add_library(ch_contrib::nlohmann_json ALIAS _nlohmann_json)
