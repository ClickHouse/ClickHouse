/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#ifndef __ZOOKEEPER_JUTE__
#define __ZOOKEEPER_JUTE__
#include "recordio.h"

#ifdef __cplusplus
extern "C" {
#endif

struct Id {
    char * scheme;
    char * id;
};
int serialize_Id(struct oarchive *out, const char *tag, struct Id *v);
int deserialize_Id(struct iarchive *in, const char *tag, struct Id*v);
void deallocate_Id(struct Id*);
struct ACL {
    int32_t perms;
    struct Id id;
};
int serialize_ACL(struct oarchive *out, const char *tag, struct ACL *v);
int deserialize_ACL(struct iarchive *in, const char *tag, struct ACL*v);
void deallocate_ACL(struct ACL*);
struct Stat {
    int64_t czxid;
    int64_t mzxid;
    int64_t ctime;
    int64_t mtime;
    int32_t version;
    int32_t cversion;
    int32_t aversion;
    int64_t ephemeralOwner;
    int32_t dataLength;
    int32_t numChildren;
    int64_t pzxid;
};
int serialize_Stat(struct oarchive *out, const char *tag, struct Stat *v);
int deserialize_Stat(struct iarchive *in, const char *tag, struct Stat*v);
void deallocate_Stat(struct Stat*);
struct StatPersisted {
    int64_t czxid;
    int64_t mzxid;
    int64_t ctime;
    int64_t mtime;
    int32_t version;
    int32_t cversion;
    int32_t aversion;
    int64_t ephemeralOwner;
    int64_t pzxid;
};
int serialize_StatPersisted(struct oarchive *out, const char *tag, struct StatPersisted *v);
int deserialize_StatPersisted(struct iarchive *in, const char *tag, struct StatPersisted*v);
void deallocate_StatPersisted(struct StatPersisted*);
struct StatPersistedV1 {
    int64_t czxid;
    int64_t mzxid;
    int64_t ctime;
    int64_t mtime;
    int32_t version;
    int32_t cversion;
    int32_t aversion;
    int64_t ephemeralOwner;
};
int serialize_StatPersistedV1(struct oarchive *out, const char *tag, struct StatPersistedV1 *v);
int deserialize_StatPersistedV1(struct iarchive *in, const char *tag, struct StatPersistedV1*v);
void deallocate_StatPersistedV1(struct StatPersistedV1*);
struct ConnectRequest {
    int32_t protocolVersion;
    int64_t lastZxidSeen;
    int32_t timeOut;
    int64_t sessionId;
    struct buffer passwd;
};
int serialize_ConnectRequest(struct oarchive *out, const char *tag, struct ConnectRequest *v);
int deserialize_ConnectRequest(struct iarchive *in, const char *tag, struct ConnectRequest*v);
void deallocate_ConnectRequest(struct ConnectRequest*);
struct ConnectResponse {
    int32_t protocolVersion;
    int32_t timeOut;
    int64_t sessionId;
    struct buffer passwd;
};
int serialize_ConnectResponse(struct oarchive *out, const char *tag, struct ConnectResponse *v);
int deserialize_ConnectResponse(struct iarchive *in, const char *tag, struct ConnectResponse*v);
void deallocate_ConnectResponse(struct ConnectResponse*);
struct String_vector {
    int32_t count;
    char * *data;

};
int serialize_String_vector(struct oarchive *out, const char *tag, struct String_vector *v);
int deserialize_String_vector(struct iarchive *in, const char *tag, struct String_vector *v);
int allocate_String_vector(struct String_vector *v, int32_t len);
int deallocate_String_vector(struct String_vector *v);
struct SetWatches {
    int64_t relativeZxid;
    struct String_vector dataWatches;
    struct String_vector existWatches;
    struct String_vector childWatches;
};
int serialize_SetWatches(struct oarchive *out, const char *tag, struct SetWatches *v);
int deserialize_SetWatches(struct iarchive *in, const char *tag, struct SetWatches*v);
void deallocate_SetWatches(struct SetWatches*);
struct RequestHeader {
    int32_t xid;
    int32_t type;
};
int serialize_RequestHeader(struct oarchive *out, const char *tag, struct RequestHeader *v);
int deserialize_RequestHeader(struct iarchive *in, const char *tag, struct RequestHeader*v);
void deallocate_RequestHeader(struct RequestHeader*);
struct MultiHeader {
    int32_t type;
    int32_t done;
    int32_t err;
};
int serialize_MultiHeader(struct oarchive *out, const char *tag, struct MultiHeader *v);
int deserialize_MultiHeader(struct iarchive *in, const char *tag, struct MultiHeader*v);
void deallocate_MultiHeader(struct MultiHeader*);
struct AuthPacket {
    int32_t type;
    char * scheme;
    struct buffer auth;
};
int serialize_AuthPacket(struct oarchive *out, const char *tag, struct AuthPacket *v);
int deserialize_AuthPacket(struct iarchive *in, const char *tag, struct AuthPacket*v);
void deallocate_AuthPacket(struct AuthPacket*);
struct ReplyHeader {
    int32_t xid;
    int64_t zxid;
    int32_t err;
};
int serialize_ReplyHeader(struct oarchive *out, const char *tag, struct ReplyHeader *v);
int deserialize_ReplyHeader(struct iarchive *in, const char *tag, struct ReplyHeader*v);
void deallocate_ReplyHeader(struct ReplyHeader*);
struct GetDataRequest {
    char * path;
    int32_t watch;
};
int serialize_GetDataRequest(struct oarchive *out, const char *tag, struct GetDataRequest *v);
int deserialize_GetDataRequest(struct iarchive *in, const char *tag, struct GetDataRequest*v);
void deallocate_GetDataRequest(struct GetDataRequest*);
struct SetDataRequest {
    char * path;
    struct buffer data;
    int32_t version;
};
int serialize_SetDataRequest(struct oarchive *out, const char *tag, struct SetDataRequest *v);
int deserialize_SetDataRequest(struct iarchive *in, const char *tag, struct SetDataRequest*v);
void deallocate_SetDataRequest(struct SetDataRequest*);
struct SetDataResponse {
    struct Stat stat;
};
int serialize_SetDataResponse(struct oarchive *out, const char *tag, struct SetDataResponse *v);
int deserialize_SetDataResponse(struct iarchive *in, const char *tag, struct SetDataResponse*v);
void deallocate_SetDataResponse(struct SetDataResponse*);
struct GetSASLRequest {
    struct buffer token;
};
int serialize_GetSASLRequest(struct oarchive *out, const char *tag, struct GetSASLRequest *v);
int deserialize_GetSASLRequest(struct iarchive *in, const char *tag, struct GetSASLRequest*v);
void deallocate_GetSASLRequest(struct GetSASLRequest*);
struct SetSASLRequest {
    struct buffer token;
};
int serialize_SetSASLRequest(struct oarchive *out, const char *tag, struct SetSASLRequest *v);
int deserialize_SetSASLRequest(struct iarchive *in, const char *tag, struct SetSASLRequest*v);
void deallocate_SetSASLRequest(struct SetSASLRequest*);
struct SetSASLResponse {
    struct buffer token;
};
int serialize_SetSASLResponse(struct oarchive *out, const char *tag, struct SetSASLResponse *v);
int deserialize_SetSASLResponse(struct iarchive *in, const char *tag, struct SetSASLResponse*v);
void deallocate_SetSASLResponse(struct SetSASLResponse*);
struct ACL_vector {
    int32_t count;
    struct ACL *data;

};
int serialize_ACL_vector(struct oarchive *out, const char *tag, struct ACL_vector *v);
int deserialize_ACL_vector(struct iarchive *in, const char *tag, struct ACL_vector *v);
int allocate_ACL_vector(struct ACL_vector *v, int32_t len);
int deallocate_ACL_vector(struct ACL_vector *v);
struct CreateRequest {
    char * path;
    struct buffer data;
    struct ACL_vector acl;
    int32_t flags;
};
int serialize_CreateRequest(struct oarchive *out, const char *tag, struct CreateRequest *v);
int deserialize_CreateRequest(struct iarchive *in, const char *tag, struct CreateRequest*v);
void deallocate_CreateRequest(struct CreateRequest*);
struct DeleteRequest {
    char * path;
    int32_t version;
};
int serialize_DeleteRequest(struct oarchive *out, const char *tag, struct DeleteRequest *v);
int deserialize_DeleteRequest(struct iarchive *in, const char *tag, struct DeleteRequest*v);
void deallocate_DeleteRequest(struct DeleteRequest*);
struct GetChildrenRequest {
    char * path;
    int32_t watch;
};
int serialize_GetChildrenRequest(struct oarchive *out, const char *tag, struct GetChildrenRequest *v);
int deserialize_GetChildrenRequest(struct iarchive *in, const char *tag, struct GetChildrenRequest*v);
void deallocate_GetChildrenRequest(struct GetChildrenRequest*);
struct GetChildren2Request {
    char * path;
    int32_t watch;
};
int serialize_GetChildren2Request(struct oarchive *out, const char *tag, struct GetChildren2Request *v);
int deserialize_GetChildren2Request(struct iarchive *in, const char *tag, struct GetChildren2Request*v);
void deallocate_GetChildren2Request(struct GetChildren2Request*);
struct CheckVersionRequest {
    char * path;
    int32_t version;
};
int serialize_CheckVersionRequest(struct oarchive *out, const char *tag, struct CheckVersionRequest *v);
int deserialize_CheckVersionRequest(struct iarchive *in, const char *tag, struct CheckVersionRequest*v);
void deallocate_CheckVersionRequest(struct CheckVersionRequest*);
struct GetMaxChildrenRequest {
    char * path;
};
int serialize_GetMaxChildrenRequest(struct oarchive *out, const char *tag, struct GetMaxChildrenRequest *v);
int deserialize_GetMaxChildrenRequest(struct iarchive *in, const char *tag, struct GetMaxChildrenRequest*v);
void deallocate_GetMaxChildrenRequest(struct GetMaxChildrenRequest*);
struct GetMaxChildrenResponse {
    int32_t max;
};
int serialize_GetMaxChildrenResponse(struct oarchive *out, const char *tag, struct GetMaxChildrenResponse *v);
int deserialize_GetMaxChildrenResponse(struct iarchive *in, const char *tag, struct GetMaxChildrenResponse*v);
void deallocate_GetMaxChildrenResponse(struct GetMaxChildrenResponse*);
struct SetMaxChildrenRequest {
    char * path;
    int32_t max;
};
int serialize_SetMaxChildrenRequest(struct oarchive *out, const char *tag, struct SetMaxChildrenRequest *v);
int deserialize_SetMaxChildrenRequest(struct iarchive *in, const char *tag, struct SetMaxChildrenRequest*v);
void deallocate_SetMaxChildrenRequest(struct SetMaxChildrenRequest*);
struct SyncRequest {
    char * path;
};
int serialize_SyncRequest(struct oarchive *out, const char *tag, struct SyncRequest *v);
int deserialize_SyncRequest(struct iarchive *in, const char *tag, struct SyncRequest*v);
void deallocate_SyncRequest(struct SyncRequest*);
struct SyncResponse {
    char * path;
};
int serialize_SyncResponse(struct oarchive *out, const char *tag, struct SyncResponse *v);
int deserialize_SyncResponse(struct iarchive *in, const char *tag, struct SyncResponse*v);
void deallocate_SyncResponse(struct SyncResponse*);
struct GetACLRequest {
    char * path;
};
int serialize_GetACLRequest(struct oarchive *out, const char *tag, struct GetACLRequest *v);
int deserialize_GetACLRequest(struct iarchive *in, const char *tag, struct GetACLRequest*v);
void deallocate_GetACLRequest(struct GetACLRequest*);
struct SetACLRequest {
    char * path;
    struct ACL_vector acl;
    int32_t version;
};
int serialize_SetACLRequest(struct oarchive *out, const char *tag, struct SetACLRequest *v);
int deserialize_SetACLRequest(struct iarchive *in, const char *tag, struct SetACLRequest*v);
void deallocate_SetACLRequest(struct SetACLRequest*);
struct SetACLResponse {
    struct Stat stat;
};
int serialize_SetACLResponse(struct oarchive *out, const char *tag, struct SetACLResponse *v);
int deserialize_SetACLResponse(struct iarchive *in, const char *tag, struct SetACLResponse*v);
void deallocate_SetACLResponse(struct SetACLResponse*);
struct WatcherEvent {
    int32_t type;
    int32_t state;
    char * path;
};
int serialize_WatcherEvent(struct oarchive *out, const char *tag, struct WatcherEvent *v);
int deserialize_WatcherEvent(struct iarchive *in, const char *tag, struct WatcherEvent*v);
void deallocate_WatcherEvent(struct WatcherEvent*);
struct ErrorResponse {
    int32_t err;
};
int serialize_ErrorResponse(struct oarchive *out, const char *tag, struct ErrorResponse *v);
int deserialize_ErrorResponse(struct iarchive *in, const char *tag, struct ErrorResponse*v);
void deallocate_ErrorResponse(struct ErrorResponse*);
struct CreateResponse {
    char * path;
};
int serialize_CreateResponse(struct oarchive *out, const char *tag, struct CreateResponse *v);
int deserialize_CreateResponse(struct iarchive *in, const char *tag, struct CreateResponse*v);
void deallocate_CreateResponse(struct CreateResponse*);
struct ExistsRequest {
    char * path;
    int32_t watch;
};
int serialize_ExistsRequest(struct oarchive *out, const char *tag, struct ExistsRequest *v);
int deserialize_ExistsRequest(struct iarchive *in, const char *tag, struct ExistsRequest*v);
void deallocate_ExistsRequest(struct ExistsRequest*);
struct ExistsResponse {
    struct Stat stat;
};
int serialize_ExistsResponse(struct oarchive *out, const char *tag, struct ExistsResponse *v);
int deserialize_ExistsResponse(struct iarchive *in, const char *tag, struct ExistsResponse*v);
void deallocate_ExistsResponse(struct ExistsResponse*);
struct GetDataResponse {
    struct buffer data;
    struct Stat stat;
};
int serialize_GetDataResponse(struct oarchive *out, const char *tag, struct GetDataResponse *v);
int deserialize_GetDataResponse(struct iarchive *in, const char *tag, struct GetDataResponse*v);
void deallocate_GetDataResponse(struct GetDataResponse*);
struct GetChildrenResponse {
    struct String_vector children;
};
int serialize_GetChildrenResponse(struct oarchive *out, const char *tag, struct GetChildrenResponse *v);
int deserialize_GetChildrenResponse(struct iarchive *in, const char *tag, struct GetChildrenResponse*v);
void deallocate_GetChildrenResponse(struct GetChildrenResponse*);
struct GetChildren2Response {
    struct String_vector children;
    struct Stat stat;
};
int serialize_GetChildren2Response(struct oarchive *out, const char *tag, struct GetChildren2Response *v);
int deserialize_GetChildren2Response(struct iarchive *in, const char *tag, struct GetChildren2Response*v);
void deallocate_GetChildren2Response(struct GetChildren2Response*);
struct GetACLResponse {
    struct ACL_vector acl;
    struct Stat stat;
};
int serialize_GetACLResponse(struct oarchive *out, const char *tag, struct GetACLResponse *v);
int deserialize_GetACLResponse(struct iarchive *in, const char *tag, struct GetACLResponse*v);
void deallocate_GetACLResponse(struct GetACLResponse*);
struct LearnerInfo {
    int64_t serverid;
    int32_t protocolVersion;
};
int serialize_LearnerInfo(struct oarchive *out, const char *tag, struct LearnerInfo *v);
int deserialize_LearnerInfo(struct iarchive *in, const char *tag, struct LearnerInfo*v);
void deallocate_LearnerInfo(struct LearnerInfo*);
struct Id_vector {
    int32_t count;
    struct Id *data;

};
int serialize_Id_vector(struct oarchive *out, const char *tag, struct Id_vector *v);
int deserialize_Id_vector(struct iarchive *in, const char *tag, struct Id_vector *v);
int allocate_Id_vector(struct Id_vector *v, int32_t len);
int deallocate_Id_vector(struct Id_vector *v);
struct QuorumPacket {
    int32_t type;
    int64_t zxid;
    struct buffer data;
    struct Id_vector authinfo;
};
int serialize_QuorumPacket(struct oarchive *out, const char *tag, struct QuorumPacket *v);
int deserialize_QuorumPacket(struct iarchive *in, const char *tag, struct QuorumPacket*v);
void deallocate_QuorumPacket(struct QuorumPacket*);
struct FileHeader {
    int32_t magic;
    int32_t version;
    int64_t dbid;
};
int serialize_FileHeader(struct oarchive *out, const char *tag, struct FileHeader *v);
int deserialize_FileHeader(struct iarchive *in, const char *tag, struct FileHeader*v);
void deallocate_FileHeader(struct FileHeader*);
struct TxnHeader {
    int64_t clientId;
    int32_t cxid;
    int64_t zxid;
    int64_t time;
    int32_t type;
};
int serialize_TxnHeader(struct oarchive *out, const char *tag, struct TxnHeader *v);
int deserialize_TxnHeader(struct iarchive *in, const char *tag, struct TxnHeader*v);
void deallocate_TxnHeader(struct TxnHeader*);
struct CreateTxnV0 {
    char * path;
    struct buffer data;
    struct ACL_vector acl;
    int32_t ephemeral;
};
int serialize_CreateTxnV0(struct oarchive *out, const char *tag, struct CreateTxnV0 *v);
int deserialize_CreateTxnV0(struct iarchive *in, const char *tag, struct CreateTxnV0*v);
void deallocate_CreateTxnV0(struct CreateTxnV0*);
struct CreateTxn {
    char * path;
    struct buffer data;
    struct ACL_vector acl;
    int32_t ephemeral;
    int32_t parentCVersion;
};
int serialize_CreateTxn(struct oarchive *out, const char *tag, struct CreateTxn *v);
int deserialize_CreateTxn(struct iarchive *in, const char *tag, struct CreateTxn*v);
void deallocate_CreateTxn(struct CreateTxn*);
struct DeleteTxn {
    char * path;
};
int serialize_DeleteTxn(struct oarchive *out, const char *tag, struct DeleteTxn *v);
int deserialize_DeleteTxn(struct iarchive *in, const char *tag, struct DeleteTxn*v);
void deallocate_DeleteTxn(struct DeleteTxn*);
struct SetDataTxn {
    char * path;
    struct buffer data;
    int32_t version;
};
int serialize_SetDataTxn(struct oarchive *out, const char *tag, struct SetDataTxn *v);
int deserialize_SetDataTxn(struct iarchive *in, const char *tag, struct SetDataTxn*v);
void deallocate_SetDataTxn(struct SetDataTxn*);
struct CheckVersionTxn {
    char * path;
    int32_t version;
};
int serialize_CheckVersionTxn(struct oarchive *out, const char *tag, struct CheckVersionTxn *v);
int deserialize_CheckVersionTxn(struct iarchive *in, const char *tag, struct CheckVersionTxn*v);
void deallocate_CheckVersionTxn(struct CheckVersionTxn*);
struct SetACLTxn {
    char * path;
    struct ACL_vector acl;
    int32_t version;
};
int serialize_SetACLTxn(struct oarchive *out, const char *tag, struct SetACLTxn *v);
int deserialize_SetACLTxn(struct iarchive *in, const char *tag, struct SetACLTxn*v);
void deallocate_SetACLTxn(struct SetACLTxn*);
struct SetMaxChildrenTxn {
    char * path;
    int32_t max;
};
int serialize_SetMaxChildrenTxn(struct oarchive *out, const char *tag, struct SetMaxChildrenTxn *v);
int deserialize_SetMaxChildrenTxn(struct iarchive *in, const char *tag, struct SetMaxChildrenTxn*v);
void deallocate_SetMaxChildrenTxn(struct SetMaxChildrenTxn*);
struct CreateSessionTxn {
    int32_t timeOut;
};
int serialize_CreateSessionTxn(struct oarchive *out, const char *tag, struct CreateSessionTxn *v);
int deserialize_CreateSessionTxn(struct iarchive *in, const char *tag, struct CreateSessionTxn*v);
void deallocate_CreateSessionTxn(struct CreateSessionTxn*);
struct ErrorTxn {
    int32_t err;
};
int serialize_ErrorTxn(struct oarchive *out, const char *tag, struct ErrorTxn *v);
int deserialize_ErrorTxn(struct iarchive *in, const char *tag, struct ErrorTxn*v);
void deallocate_ErrorTxn(struct ErrorTxn*);
struct Txn {
    int32_t type;
    struct buffer data;
};
int serialize_Txn(struct oarchive *out, const char *tag, struct Txn *v);
int deserialize_Txn(struct iarchive *in, const char *tag, struct Txn*v);
void deallocate_Txn(struct Txn*);
struct Txn_vector {
    int32_t count;
    struct Txn *data;

};
int serialize_Txn_vector(struct oarchive *out, const char *tag, struct Txn_vector *v);
int deserialize_Txn_vector(struct iarchive *in, const char *tag, struct Txn_vector *v);
int allocate_Txn_vector(struct Txn_vector *v, int32_t len);
int deallocate_Txn_vector(struct Txn_vector *v);
struct MultiTxn {
    struct Txn_vector txns;
};
int serialize_MultiTxn(struct oarchive *out, const char *tag, struct MultiTxn *v);
int deserialize_MultiTxn(struct iarchive *in, const char *tag, struct MultiTxn*v);
void deallocate_MultiTxn(struct MultiTxn*);

#ifdef __cplusplus
}
#endif

#endif //ZOOKEEPER_JUTE__
