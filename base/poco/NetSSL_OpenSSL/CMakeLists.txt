if (ENABLE_SSL)
    file (GLOB SRCS src/*.cpp)

    add_library (_poco_net_ssl ${SRCS})
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    target_include_directories (_poco_net_ssl SYSTEM PUBLIC "include")
    target_link_libraries (_poco_net_ssl PUBLIC Poco::Crypto Poco::Net Poco::Util)
else ()
    add_library (_poco_net_ssl INTERFACE)
    add_library (Poco::Net::SSL ALIAS _poco_net_ssl)

    message (STATUS "Not using Poco::Net::SSL")
endif ()
