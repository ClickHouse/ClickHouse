LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
    contrib/libs/protobuf_std
)

SRCS(
    FormatFactory.cpp
    NativeFormat.cpp
    NullFormat.cpp
)

END()
