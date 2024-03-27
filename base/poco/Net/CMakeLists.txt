file (GLOB SRCS src/*.cpp)

add_library (_poco_net ${SRCS})
add_library (Poco::Net ALIAS _poco_net)

if (OS_LINUX)
    target_compile_definitions (_poco_net PUBLIC POCO_HAVE_FD_EPOLL)
elseif (OS_DARWIN OR OS_FREEBSD)
    target_compile_definitions (_poco_net PUBLIC POCO_HAVE_FD_POLL)
endif ()

if (OS_SUNOS)
    target_link_libraries (_poco_net PUBLIC socket nsl)
endif ()

# TODO: remove these warning exclusions
target_compile_options (_poco_net
    PRIVATE
        -Wno-atomic-implicit-seq-cst
        -Wno-cast-align
        -Wno-cast-qual
        -Wno-comma
        -Wno-covered-switch-default
        -Wno-deprecated
        -Wno-extra-semi
        -Wno-extra-semi-stmt
        -Wno-missing-noreturn
        -Wno-old-style-cast
        -Wno-shadow
        -Wno-shorten-64-to-32
        -Wno-sign-compare
        -Wno-suggest-destructor-override
        -Wno-suggest-override
        -Wno-undef
        -Wno-unreachable-code
        -Wno-unreachable-code-return
        -Wno-unused-macros
        -Wno-unused-parameter
        -Wno-zero-as-null-pointer-constant
)

target_include_directories (_poco_net SYSTEM PUBLIC "include")
target_link_libraries (_poco_net PUBLIC Poco::Foundation)
