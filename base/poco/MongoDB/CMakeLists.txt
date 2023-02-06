set (SRCS
    src/Array.cpp
    src/Binary.cpp
    src/Connection.cpp
    src/Cursor.cpp
    src/Database.cpp
    src/DeleteRequest.cpp
    src/Document.cpp
    src/Element.cpp
    src/GetMoreRequest.cpp
    src/InsertRequest.cpp
    src/JavaScriptCode.cpp
    src/KillCursorsRequest.cpp
    src/Message.cpp
    src/MessageHeader.cpp
    src/ObjectId.cpp
    src/QueryRequest.cpp
    src/RegularExpression.cpp
    src/ReplicaSet.cpp
    src/RequestMessage.cpp
    src/ResponseMessage.cpp
    src/UpdateRequest.cpp
)

add_library (_poco_mongodb ${SRCS})
add_library (Poco::MongoDB ALIAS _poco_mongodb)

target_include_directories (_poco_mongodb SYSTEM PUBLIC "include")
target_link_libraries (_poco_mongodb PUBLIC Poco::Net)
