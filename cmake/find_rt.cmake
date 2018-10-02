if (APPLE)
    # lib from libs/libcommon
    set (RT_LIBRARY "apple_rt")
elseif (OS_FREEBSD)
    find_library (RT_LIBRARY rt)
else ()
    set (RT_LIBRARY "")
endif ()

message(STATUS "Using rt: ${RT_LIBRARY}")
