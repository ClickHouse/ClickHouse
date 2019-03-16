if(USE_GTEST)
    add_executable(column_unique column_unique.cpp)
    target_link_libraries(column_unique PRIVATE dbms ${GTEST_BOTH_LIBRARIES})
endif()