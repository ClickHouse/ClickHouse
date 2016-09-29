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
#ifndef PROTO_H_
#define PROTO_H_

#ifdef __cplusplus
extern "C" {
#endif

#define ZOO_NOTIFY_OP 0
#define ZOO_CREATE_OP 1
#define ZOO_DELETE_OP 2
#define ZOO_EXISTS_OP 3
#define ZOO_GETDATA_OP 4
#define ZOO_SETDATA_OP 5
#define ZOO_GETACL_OP 6
#define ZOO_SETACL_OP 7
#define ZOO_GETCHILDREN_OP 8
#define ZOO_SYNC_OP 9
#define ZOO_PING_OP 11
#define ZOO_GETCHILDREN2_OP 12
#define ZOO_CHECK_OP 13
#define ZOO_MULTI_OP 14
#define ZOO_CLOSE_OP -11
#define ZOO_SETAUTH_OP 100
#define ZOO_SETWATCHES_OP 101

#ifdef __cplusplus
}
#endif

#endif /*PROTO_H_*/
