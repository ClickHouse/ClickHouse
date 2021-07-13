option (USE_INTERNAL_POCO_LIBRARY "Use internal Poco library" ON)

if (NOT USE_INTERNAL_POCO_LIBRARY)
    find_path (ROOT_DIR NAMES Foundation/include/Poco/Poco.h include/Poco/Poco.h)
    if (NOT ROOT_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system poco")
    endif()
endif ()
