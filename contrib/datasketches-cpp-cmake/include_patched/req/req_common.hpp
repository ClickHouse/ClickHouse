/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef REQ_COMMON_HPP_
#define REQ_COMMON_HPP_

#include <random>

#include "serde.hpp"
#include "common_defs.hpp"

namespace datasketches {

/// REQ sketch constants
namespace req_constants {
  /// minimum value of parameter K
  const uint16_t MIN_K = 4;
  /// initial number of sections
  const uint8_t INIT_NUM_SECTIONS = 3;
  /// multiplier for nominal capacity
  const unsigned MULTIPLIER = 2;
}

} /* namespace datasketches */

#endif
