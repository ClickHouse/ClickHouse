
add_library (dict_lib SHARED "dict_lib.cpp")
add_library (dict_lib_c SHARED "dict_lib_c.c")
add_library (dict_lib_empty SHARED "dict_lib_empty.cpp")

# Don't change lib name in debug build
# because result .so will be pointed in dictionary_*.xml
set_target_properties(dict_lib PROPERTIES DEBUG_POSTFIX "")
set_target_properties(dict_lib_c PROPERTIES DEBUG_POSTFIX "")
set_target_properties(dict_lib_empty PROPERTIES DEBUG_POSTFIX "")
