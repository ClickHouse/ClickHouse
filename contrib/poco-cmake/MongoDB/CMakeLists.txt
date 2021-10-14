if (USE_INTERNAL_POCO_LIBRARY)
    set (SRCS
        ${LIBRARY_DIR}/MongoDB/src/Array.cpp
        ${LIBRARY_DIR}/MongoDB/src/Binary.cpp
        ${LIBRARY_DIR}/MongoDB/src/Connection.cpp
        ${LIBRARY_DIR}/MongoDB/src/Cursor.cpp
        ${LIBRARY_DIR}/MongoDB/src/Database.cpp
        ${LIBRARY_DIR}/MongoDB/src/DeleteRequest.cpp
        ${LIBRARY_DIR}/MongoDB/src/Document.cpp
        ${LIBRARY_DIR}/MongoDB/src/Element.cpp
        ${LIBRARY_DIR}/MongoDB/src/GetMoreRequest.cpp
        ${LIBRARY_DIR}/MongoDB/src/InsertRequest.cpp
        ${LIBRARY_DIR}/MongoDB/src/JavaScriptCode.cpp
        ${LIBRARY_DIR}/MongoDB/src/KillCursorsRequest.cpp
        ${LIBRARY_DIR}/MongoDB/src/Message.cpp
        ${LIBRARY_DIR}/MongoDB/src/MessageHeader.cpp
        ${LIBRARY_DIR}/MongoDB/src/ObjectId.cpp
        ${LIBRARY_DIR}/MongoDB/src/QueryRequest.cpp
        ${LIBRARY_DIR}/MongoDB/src/RegularExpression.cpp
        ${LIBRARY_DIR}/MongoDB/src/ReplicaSet.cpp
        ${LIBRARY_DIR}/MongoDB/src/RequestMessage.cpp
        ${LIBRARY_DIR}/MongoDB/src/ResponseMessage.cpp
        ${LIBRARY_DIR}/MongoDB/src/UpdateRequest.cpp
    )

    add_library (_poco_mongodb ${SRCS})
    add_library (Poco::MongoDB ALIAS _poco_mongodb)

    target_include_directories (_poco_mongodb SYSTEM PUBLIC ${LIBRARY_DIR}/MongoDB/include)
    target_link_libraries (_poco_mongodb PUBLIC Poco::Net)
else ()
    add_library (Poco::MongoDB UNKNOWN IMPORTED GLOBAL)

    find_library (LIBRARY_POCO_MONGODB PocoMongoDB)
    find_path (INCLUDE_POCO_MONGODB Poco/MongoDB/MongoDB.h)
    set_target_properties (Poco::MongoDB PROPERTIES IMPORTED_LOCATION ${LIBRARY_POCO_MONGODB})
    set_target_properties (Poco::MongoDB PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_POCO_MONGODB})

    message (STATUS "Using Poco::MongoDB: ${LIBRARY_POCO_MONGODB} ${INCLUDE_POCO_MONGODB}")
endif ()
