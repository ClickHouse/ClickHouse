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

target_link_libraries(zkutil libzookeeper_mt.a pthread PocoFoundation)

add_subdirectory (src)
