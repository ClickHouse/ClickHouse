OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
)


SRCS(
    DiskS3.cpp
    registerDiskS3.cpp
    ProxyListConfiguration.cpp
    ProxyResolverConfiguration.cpp
)

END()
