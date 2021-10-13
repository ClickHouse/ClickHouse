option (ENABLE_FILELOG "Enable FILELOG" ON)

if (NOT ENABLE_FILELOG)
	message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use StorageFileLog with ENABLE_FILELOG=OFF")
    return()
endif()

# StorageFileLog only support Linux platform
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set (USE_FILELOG 1)
    message (STATUS "Using StorageFileLog = 1")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

