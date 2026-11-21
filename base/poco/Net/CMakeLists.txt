file (GLOB SRCS src/*.cpp)

if (OS_WINDOWS)
    # `NetworkInterface` enumerates the host's interfaces. Upstream Poco implements it on
    # Windows through `GetAdaptersAddresses`, but that implementation relies on several members
    # this fork removed along with the rest of its Windows support (`NetworkInterfaceImpl::
    # setRunning`, the `IPAddress(const SOCKET_ADDRESS &)` constructor, the two-argument
    # `setFlags`). Nothing in ClickHouse uses `Poco::Net::NetworkInterface` - `isLocalAddress`
    # calls `getifaddrs` directly - and inside Poco::Net only `MulticastSocket` needs it, which
    # ClickHouse does not use either, so both are left out here rather than restoring code that
    # would have no caller. Reinstating them is a prerequisite for multicast on Windows.
    list (REMOVE_ITEM SRCS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/NetworkInterface.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/MulticastSocket.cpp"
    )
endif ()

add_library (_poco_net ${SRCS})
add_library (Poco::Net ALIAS _poco_net)

if (OS_LINUX)
    target_compile_definitions (_poco_net PRIVATE POCO_HAVE_FD_EPOLL)
elseif (OS_DARWIN OR OS_FREEBSD)
    target_compile_definitions (_poco_net PRIVATE POCO_HAVE_FD_POLL)
endif ()
# Note: on Windows neither is set, so `SocketImpl::pollImpl` uses its `select()` path - mingw-w64
# has no `poll()`, and Poco 1.9.3 does not know about `WSAPoll`.

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
        -Wno-implicit-int-conversion
        -Wno-implicit-int-float-conversion
)

target_include_directories (_poco_net SYSTEM PUBLIC "include")
target_link_libraries (_poco_net PUBLIC Poco::Foundation)
