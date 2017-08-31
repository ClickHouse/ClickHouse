
add_library (dictionary_library SHARED "dictionary_library.cpp")
add_library (dictionary_library_c SHARED "dictionary_library_c.c")
add_library (dictionary_library_empty SHARED "dictionary_library_empty.cpp")

# Don't change lib name in debug build
# because result .so will be pointed in dictionary_*.xml
set_target_properties(dictionary_library PROPERTIES DEBUG_POSTFIX "")
set_target_properties(dictionary_library_c PROPERTIES DEBUG_POSTFIX "")
set_target_properties(dictionary_library_empty PROPERTIES DEBUG_POSTFIX "")
