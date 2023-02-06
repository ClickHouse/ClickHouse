add_executable(DOMParser src/DOMParser.cpp)
target_link_libraries(DOMParser PUBLIC Poco::XML)
