
add_library (dictionary_library SHARED "dictionary_library.cpp")
target_include_directories (dictionary_library PRIVATE ${DBMS_INCLUDE_DIR})
add_library (dictionary_library_c SHARED "dictionary_library_c.c")
target_include_directories (dictionary_library_c PRIVATE ${DBMS_INCLUDE_DIR})
add_library (dictionary_library_empty SHARED "dictionary_library_empty.cpp")
target_include_directories (dictionary_library_empty PRIVATE ${DBMS_INCLUDE_DIR})

# Don't add "lib" prefix, and don't change lib name in debug build
# because result .so will be pointed in dictionary_*.xml
set_target_properties(dictionary_library PROPERTIES PREFIX "" DEBUG_POSTFIX "")
set_target_properties(dictionary_library_c PROPERTIES PREFIX "" DEBUG_POSTFIX "")
set_target_properties(dictionary_library_empty PROPERTIES PREFIX "" DEBUG_POSTFIX "")
