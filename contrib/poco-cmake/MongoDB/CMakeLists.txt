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

    add_library (poco_mongodb ${SRCS})

    target_include_directories (poco_mongodb PUBLIC ${LIBRARY_DIR}/MongoDB/include)
    target_link_libraries (poco_mongodb PUBLIC poco_net)
endif ()
