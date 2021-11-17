# StorageFileLog only support Linux platform
if (OS_LINUX)
    set (USE_FILELOG 1)
    message (STATUS "Using StorageFileLog = 1")
else()
    message(STATUS "StorageFileLog is only supported on Linux")
endif ()

