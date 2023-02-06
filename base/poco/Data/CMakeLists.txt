file (GLOB SRCS src/*.cpp)

add_library (_poco_data ${SRCS})
add_library (Poco::Data ALIAS _poco_data)

target_include_directories (_poco_data SYSTEM PUBLIC "include")
target_link_libraries (_poco_data PUBLIC Poco::Foundation)
