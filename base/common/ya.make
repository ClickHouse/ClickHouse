LIBRARY()

PEERDIR(
    contrib/libs/poco/Net
    contrib/libs/poco/Util
)

SRCS(
    argsToConfig.cpp
    getFQDNOrHostName.cpp
    coverage.cpp
    getMemoryAmount.cpp
    JSON.cpp
)

END()
