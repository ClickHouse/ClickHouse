include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (date_lut_init date_lut_init.cpp)
add_executable (date_lut2 date_lut2.cpp)
add_executable (date_lut3 date_lut3.cpp)
add_executable (date_lut4 date_lut4.cpp)
add_executable (multi_version multi_version.cpp)
add_executable (json_test json_test.cpp)

target_link_libraries (date_lut_init common libicui18n.a libicuuc.a libicudata.a dl)
target_link_libraries (date_lut2 common libicui18n.a libicuuc.a libicudata.a dl)
target_link_libraries (date_lut3 common libicui18n.a libicuuc.a libicudata.a dl)
target_link_libraries (date_lut4 common libicui18n.a libicuuc.a libicudata.a dl)
target_link_libraries (multi_version common libboost_thread.a libboost_system.a rt)
target_link_libraries (json_test dbms libboost_thread.a libboost_system.a rt)

add_check (json_test)
