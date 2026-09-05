// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef NANOARROW_CONFIG_H_INCLUDED
#define NANOARROW_CONFIG_H_INCLUDED

#define NANOARROW_VERSION_MAJOR 0
#define NANOARROW_VERSION_MINOR 8
#define NANOARROW_VERSION_PATCH 0
#define NANOARROW_VERSION "0.8.0"

#define NANOARROW_VERSION_INT                                        \
  (NANOARROW_VERSION_MAJOR * 10000 + NANOARROW_VERSION_MINOR * 100 + \
   NANOARROW_VERSION_PATCH)

// #define NANOARROW_NAMESPACE

#if !defined(NANOARROW_CXX_NAMESPACE)
#define NANOARROW_CXX_NAMESPACE nanoarrow
#endif

#define NANOARROW_CXX_NAMESPACE_BEGIN namespace NANOARROW_CXX_NAMESPACE {
#define NANOARROW_CXX_NAMESPACE_END }

#endif
