set(BSON_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/bson-cpp")

set(BSON_SRC
        ${BSON_SOURCE_DIR}/src/bson_db.h
        ${BSON_SOURCE_DIR}/src/bson-inl.h
        ${BSON_SOURCE_DIR}/src/bson.h
        ${BSON_SOURCE_DIR}/src/bsonassert.h
        ${BSON_SOURCE_DIR}/src/bsonelement.h
        ${BSON_SOURCE_DIR}/src/bsonmisc.h
        ${BSON_SOURCE_DIR}/src/bsonobj.h
        ${BSON_SOURCE_DIR}/src/bsonobj.cpp
        ${BSON_SOURCE_DIR}/src/bsonobjbuilder.h
        ${BSON_SOURCE_DIR}/src/bsonobjiterator.h
        ${BSON_SOURCE_DIR}/src/bsontypes.h
        ${BSON_SOURCE_DIR}/src/inline_decls.h
        ${BSON_SOURCE_DIR}/src/inline_decls.h
        ${BSON_SOURCE_DIR}/src/oid.cpp
        ${BSON_SOURCE_DIR}/src/oid.h
        ${BSON_SOURCE_DIR}/src/ordering.h
        ${BSON_SOURCE_DIR}/src/stringdata.h

        ${BSON_SOURCE_DIR}/src/util/builder.h
        ${BSON_SOURCE_DIR}/src/util/embedded_builder.h
        ${BSON_SOURCE_DIR}/src/util/hex.h
        ${BSON_SOURCE_DIR}/src/util/json.h
        ${BSON_SOURCE_DIR}/src/util/json.cpp
        ${BSON_SOURCE_DIR}/src/util/misc.h
        ${BSON_SOURCE_DIR}/src/util/optime.h
        ${BSON_SOURCE_DIR}/src/util/time_support.h
        
        ${BSON_SOURCE_DIR}/lib/atomic_int.h
        ${BSON_SOURCE_DIR}/lib/base64.h
        ${BSON_SOURCE_DIR}/lib/base64.cpp
        ${BSON_SOURCE_DIR}/lib/md5.c
        ${BSON_SOURCE_DIR}/lib/md5.h
        ${BSON_SOURCE_DIR}/lib/md5.hpp
        ${BSON_SOURCE_DIR}/lib/nonce.cpp
        ${BSON_SOURCE_DIR}/lib/nonce.h
)

add_library(bson-cpp ${BSON_SRC})

add_library(ch_contrib::bson-cpp ALIAS bson-cpp)
