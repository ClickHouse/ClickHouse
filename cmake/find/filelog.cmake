option (ENABLE_FILELOG "Enable FILELOG" ON)

if (NOT ENABLE_FILELOG)
	message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use StorageFileLog with ENABLE_FILELOG=OFF")
    return()
endif()

# StorageFileLog only support Linux platform
if (OS_LINUX)
    set (USE_FILELOG 1)
    message (STATUS "Using StorageFileLog = 1")
else()
    message(STATUS "StorageFileLog is only supported on Linux")
endif ()

