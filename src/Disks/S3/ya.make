LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

CFLAGS(-g0)

SRCS(
    DiskS3.cpp
    registerDiskS3.cpp
    ProxyListConfiguration.cpp
    ProxyResolverConfiguration.cpp
)

END()
