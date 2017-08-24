
add_library (dict_lib SHARED "dict_lib.cpp")
#target_link_libraries(dict_lib common) # debug helpers

add_library (dict_lib_c SHARED "dict_lib_c.c")


add_library (dict_lib_empty SHARED "dict_lib_empty.cpp")

#Dont change lib name in Debug suild
set_target_properties(dict_lib PROPERTIES DEBUG_POSTFIX "")
set_target_properties(dict_lib_c PROPERTIES DEBUG_POSTFIX "")
set_target_properties(dict_lib_empty PROPERTIES DEBUG_POSTFIX "")
