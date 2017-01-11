add_library(zkutil
	src/ZooKeeper.cpp
	src/Lock.cpp
	src/SingleBarrier.cpp
	src/RWLock.cpp
	src/ZooKeeperHolder.cpp

	include/zkutil/Increment.h
    include/zkutil/LeaderElection.h
    include/zkutil/KeeperException.h
    include/zkutil/Common.h
    include/zkutil/Lock.h
    include/zkutil/SingleBarrier.h
    include/zkutil/RWLock.h
    include/zkutil/ZooKeeper.h
    include/zkutil/Types.h
    include/zkutil/ZooKeeperHolder.h)

find_package (Threads)

target_link_libraries(zkutil zookeeper  PocoFoundation ${CMAKE_THREAD_LIBS_INIT} string_utils)

add_subdirectory (src)
