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

#ifndef ORC_CONFIG_HH
#define ORC_CONFIG_HH

#define ORC_VERSION ""

#define ORC_CXX_HAS_CSTDINT

#ifdef ORC_CXX_HAS_CSTDINT
  #include <cstdint>
#else
  #include <stdint.h>
#endif

// Following MACROS should be keeped for backward compatibility.
#define ORC_NOEXCEPT noexcept
#define ORC_NULLPTR nullptr
#define ORC_OVERRIDE override
#define ORC_UNIQUE_PTR std::unique_ptr

#endif
