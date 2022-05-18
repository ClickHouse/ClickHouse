/*
 * fdb_c_types.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FDB_C_TYPES_H
#define FDB_C_TYPES_H
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Pointers to these opaque types represent objects in the FDB API */
typedef struct FDB_future FDBFuture;
typedef struct FDB_result FDBResult;
typedef struct FDB_cluster FDBCluster;
typedef struct FDB_database FDBDatabase;
typedef struct FDB_tenant FDBTenant;
typedef struct FDB_transaction FDBTransaction;

typedef int fdb_error_t;
typedef int fdb_bool_t;

#ifdef __cplusplus
}
#endif
#endif
