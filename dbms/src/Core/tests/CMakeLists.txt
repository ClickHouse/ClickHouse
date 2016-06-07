add_executable (exception exception.cpp)
target_link_libraries (exception dbms)

add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool dbms)

add_executable (field field.cpp)
target_link_libraries (field dbms)

add_executable (rvo_test rvo_test.cpp)
