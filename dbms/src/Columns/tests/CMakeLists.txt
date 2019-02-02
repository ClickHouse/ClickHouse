if(USE_GTEST)
    add_executable(column_unique column_unique.cpp)
    target_link_libraries(column_unique PRIVATE dbms gtest_main)
endif()