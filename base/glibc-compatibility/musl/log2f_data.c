/*
 * Data definition for log2f.
 *
 * Copyright (c) 2017-2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */

#include "log2f_data.h"

const struct log2f_data __log2f_data = {
  .tab = {
  { 0x1.661ec79f8f3bep+0, -0x1.efec65b963019p-2 },
  { 0x1.571ed4aaf883dp+0, -0x1.b0b6832d4fca4p-2 },
  { 0x1.49539f0f010bp+0, -0x1.7418b0a1fb77bp-2 },
  { 0x1.3c995b0b80385p+0, -0x1.39de91a6dcf7bp-2 },
  { 0x1.30d190c8864a5p+0, -0x1.01d9bf3f2b631p-2 },
  { 0x1.25e227b0b8eap+0, -0x1.97c1d1b3b7afp-3 },
  { 0x1.1bb4a4a1a343fp+0, -0x1.2f9e393af3c9fp-3 },
  { 0x1.12358f08ae5bap+0, -0x1.960cbbf788d5cp-4 },
  { 0x1.0953f419900a7p+0, -0x1.a6f9db6475fcep-5 },
  { 0x1p+0, 0x0p+0 },
  { 0x1.e608cfd9a47acp-1, 0x1.338ca9f24f53dp-4 },
  { 0x1.ca4b31f026aap-1, 0x1.476a9543891bap-3 },
  { 0x1.b2036576afce6p-1, 0x1.e840b4ac4e4d2p-3 },
  { 0x1.9c2d163a1aa2dp-1, 0x1.40645f0c6651cp-2 },
  { 0x1.886e6037841edp-1, 0x1.88e9c2c1b9ff8p-2 },
  { 0x1.767dcf5534862p-1, 0x1.ce0a44eb17bccp-2 },
  },
  .poly = {
  -0x1.712b6f70a7e4dp-2, 0x1.ecabf496832ep-2, -0x1.715479ffae3dep-1,
  0x1.715475f35c8b8p0,
  }
};
