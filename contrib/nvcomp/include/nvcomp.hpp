/*
 * SPDX-FileCopyrightText: Copyright (c) 2020-2025 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved. SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
*/

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

#include "nvcomp.h"

namespace nvcomp
{

/**
 * @brief nvCOMP supported compression formats.
*/
enum class nvcompFormatType_t : uint8_t
{
  LZ4 = 0,
  Snappy = 1,
  ANS = 2,
  GDeflate = 3,
  Cascaded = 4,
  Bitcomp = 5,
  Zstd = 6,
  Deflate = 7,
  Gzip = 8,
  NotSupportedError = 255
};

/**
 * @brief The top-level exception thrown by nvcomp C++ methods.
 */
class NVCompException : public std::runtime_error
{
public:
  /**
   * @brief Create a new NVCompException.
   *
   * @param[in] err The error associated with the exception.
   * @param[in] msg The error message.
   */
  NVCompException(nvcompStatus_t err, const std::string &msg)
      : std::runtime_error(msg + " : code=" + std::to_string(err) + ".")
      , m_err(err)
  {}

  nvcompStatus_t get_error() const noexcept { return m_err; }

private:
  nvcompStatus_t m_err;
};

#ifndef DOXYGEN_SHOULD_SKIP_THIS

/**
 * @brief Retrieve the applicable nvCOMP type for a standard type.
 *
 * @tparam T A standard C/C++ type.
 *
 * @return The applicable nvCOMP type.
 */
template <typename T>
constexpr __device__ __host__ nvcompType_t TypeOfConst() noexcept
{
  return std::is_same<T, int8_t>::value
           ? NVCOMP_TYPE_CHAR
           : (std::is_same<T, uint8_t>::value
                ? NVCOMP_TYPE_UCHAR
                : (std::is_same<T, int16_t>::value
                     ? NVCOMP_TYPE_SHORT
                     : (std::is_same<T, uint16_t>::value
                          ? NVCOMP_TYPE_USHORT
                          : (std::is_same<T, int32_t>::value
                               ? NVCOMP_TYPE_INT
                               : (std::is_same<T, uint32_t>::value
                                    ? NVCOMP_TYPE_UINT
                                    : (std::is_same<T, int64_t>::value
                                         ? NVCOMP_TYPE_LONGLONG
                                         : (std::is_same<T, uint64_t>::value ? NVCOMP_TYPE_ULONGLONG
                                                                             : (NVCOMP_TYPE_BITS))))))));
}

/**
 * @brief Retrieve the applicable nvCOMP type for a standard type with checks.
 *
 * @tparam T A standard C/C++ type.
 *
 * @return The applicable nvCOMP type.
 */
template <typename T>
inline nvcompType_t TypeOf()
{
  auto type = TypeOfConst<T>();
  if (type != NVCOMP_TYPE_BITS)
  {
    return type;
  }
  throw NVCompException(nvcompErrorNotSupported, "nvCOMP does not support the given type.");
}

#endif // DOXYGEN_SHOULD_SKIP_THIS

} // namespace nvcomp
