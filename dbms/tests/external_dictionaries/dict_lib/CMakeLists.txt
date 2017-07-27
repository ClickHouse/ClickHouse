
add_library (dict_lib SHARED "dict_lib.cpp")
target_link_libraries(dict_lib common) # debug helpers

add_library (dict_lib_c SHARED "dict_lib_c.c")

add_library (empty SHARED "empty.cpp")
