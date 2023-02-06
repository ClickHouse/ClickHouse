add_executable(DOMWriter src/DOMWriter.cpp)
target_link_libraries(DOMWriter PUBLIC Poco::XML)
