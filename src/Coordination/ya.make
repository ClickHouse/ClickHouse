# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/NuRaft
)


SRCS(
    InMemoryLogStore.cpp
    InMemoryStateManager.cpp
    NuKeeperServer.cpp
    NuKeeperStateMachine.cpp
    NuKeeperStorage.cpp
    NuKeeperStorageDispatcher.cpp
    NuKeeperStorageSerializer.cpp
    SummingStateMachine.cpp
    WriteBufferFromNuraftBuffer.cpp

)

END()
