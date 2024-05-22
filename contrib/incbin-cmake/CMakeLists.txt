set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/incbin")
add_library(_incbin INTERFACE)
target_include_directories(_incbin SYSTEM INTERFACE ${LIBRARY_DIR})
add_library(ch_contrib::incbin ALIAS _incbin)

# Warning "incbin is incompatible with bitcode. Using the library will break upload to App Store if you have bitcode enabled.
# Add `#define INCBIN_SILENCE_BITCODE_WARNING` before including this header to silence this warning."
target_compile_definitions(_incbin INTERFACE INCBIN_SILENCE_BITCODE_WARNING)
