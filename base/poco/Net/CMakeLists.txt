file (GLOB SRCS src/*.cpp)

add_library (_poco_net ${SRCS})
add_library (Poco::Net ALIAS _poco_net)

if (OS_LINUX)
    target_compile_definitions (_poco_net PUBLIC POCO_HAVE_FD_EPOLL)
elseif (OS_DARWIN OR OS_FREEBSD)
    target_compile_definitions (_poco_net PUBLIC POCO_HAVE_FD_POLL)
endif ()

target_compile_options (_poco_net
    PRIVATE
        -Wno-atomic-implicit-seq-cst
        -Wno-extra-semi-stmt
        -Wno-extra-semi
)

target_compile_options (_poco_net
    PRIVATE
        -Wno-deprecated
        -Wno-extra-semi
)
target_include_directories (_poco_net SYSTEM PUBLIC "include")
target_link_libraries (_poco_net PUBLIC Poco::Foundation)
