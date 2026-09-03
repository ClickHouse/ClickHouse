/*
 * SPDX-FileCopyrightText: Copyright (c) 2018-2025 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved. SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
*/

#ifndef DOXYGEN_SHOULD_SKIP_THIS
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#ifndef __NVCC__
#define NVCOMP_HOST_DEVICE_FUNCTION
#else
#define NVCOMP_HOST_DEVICE_FUNCTION __host__ __device__
#endif // __NVCC__

namespace nvcomp
{

/**
 * @brief Return the ceiling of the ratio of input num and input chunk.
 *
 * @tparam U The type of the argument num.
 * @tparam T The type of the argument chunk.
 * @param[in] num The dividend.
 * @param[in] chunk The divisor.
 *
 * @return The rounded quotient of the division.
 */
template <typename U, typename T>
constexpr NVCOMP_HOST_DEVICE_FUNCTION U roundUpDiv(const U num, const T chunk) noexcept
{
  return (num + chunk - 1) / chunk;
}

/**
 * @brief Round down the input num to an integer multiple of the input chunk.
 *
 * @tparam U The type of the argument num.
 * @tparam T The type of the argument chunk.
 * @param[in] num The original amount to be rounded down.
 * @param[in] chunk The rounding multiple.
 *
 * @return The rounded-down input.
 */
template <typename U, typename T>
constexpr NVCOMP_HOST_DEVICE_FUNCTION U roundDownTo(const U num, const T chunk) noexcept
{
  return (num / chunk) * chunk;
}

/**
 * @brief Round up the input num to an integer multiple of the input chunk.
 *
 * @tparam U The type of the argument num.
 * @tparam T The type of the argument chunk.
 * @param[in] num The original amount to be rounded up.
 * @param[in] chunk The rounding multiple.
 *
 * @return The rounded-up input.
 */
template <typename U, typename T>
constexpr NVCOMP_HOST_DEVICE_FUNCTION U roundUpTo(const U num, const T chunk) noexcept
{
  return roundUpDiv(num, chunk) * chunk;
}

/**
 * @brief Calculate the first aligned location after `ptr`.
 *
 * @tparam T Type such that the alignment requirement is satisfied.
 * @param[in] ptr Input pointer.
 *
 * @return The first pointer after `ptr` that satisfies the alignment requirement.
 */
template <typename T>
constexpr NVCOMP_HOST_DEVICE_FUNCTION T *roundUpToAlignment(void *ptr) noexcept
{
  constexpr auto alignment = alignof(T);
  const auto address = reinterpret_cast<uintptr_t>(ptr);
  return reinterpret_cast<T *>((address + alignment - 1) & ~(alignment - 1));
}

/**
 * @brief Calculate the first aligned location after `ptr`.
 *
 * @tparam T Type such that the alignment requirement is satisfied.
 * @param[in] ptr Input pointer pointing to constant data.
 *
 * @return The first pointer after `ptr` that satisfies the alignment requirement.
 */
template <typename T>
constexpr NVCOMP_HOST_DEVICE_FUNCTION const T *roundUpToAlignment(const void *ptr) noexcept
{
  constexpr auto alignment = alignof(T);
  const auto address = reinterpret_cast<uintptr_t>(ptr);
  return reinterpret_cast<const T *>((address + alignment - 1) & ~(alignment - 1));
}

/**
 * @brief Verifies whether a given cast from InputT type to OutputT type is valid.
 *
 * @tparam OutputT The output type we intend to cast to.
 * @tparam InputT The input type we intend to cast from.
 *
 * @return Boolean indicating whether the cast is valid.
 */
template <typename OutputT, typename InputT>
constexpr NVCOMP_HOST_DEVICE_FUNCTION bool is_cast_valid(const InputT i) noexcept
{
  static_assert(
    std::numeric_limits<OutputT>::is_integer && std::numeric_limits<InputT>::is_integer,
    "Types for is_cast_valid must both be integers"
  );
  if (std::is_unsigned<InputT>::value)
  {
    // The minimum bound is always satisfied, so just check the maximum bound.
    // Use larger type, breaking tie with InputT, which is already known unsigned.
    using largerT = typename std::conditional<(sizeof(OutputT) > sizeof(InputT)), OutputT, InputT>::type;
    return static_cast<largerT>(i) <= static_cast<largerT>((std::numeric_limits<OutputT>::max)());
  }

  // At this point, InputT is signed, but because this code will still be compiled
  // for unsigned InputT, force InputT to be signed, to avoid warnings about signed
  // vs. unsigned comparison.
  using signedInputT = typename std::make_signed<InputT>::type;
  using signedOutputT = typename std::make_signed<OutputT>::type;

  // Check whether the input is less than the minimum value of OutputT.
  // I.e. a negative signed integer is casting to an unsigned
  // Note, if OutputT is unsigned, the minimum is zero, which is safe to cast to
  // a signed type.
  if (static_cast<signedInputT>(i) < static_cast<signedOutputT>((std::numeric_limits<OutputT>::min)()))
  {
    return false;
  }

  // Because we've already checked whether the inputT is "too negative", if it's
  // negative at all this is valid
  // InputT is signed and larger than the minimum value of OutputT.
  if (static_cast<signedInputT>(i) <= static_cast<signedInputT>(0))
  {
    return true;
  }

  // InputT is signed, but larger than zero, so can be cast to unsigned.
  using unsignedInputT = typename std::make_unsigned<InputT>::type;
  using unsignedOutputT = typename std::make_unsigned<OutputT>::type;

  return static_cast<unsignedInputT>(i) <= static_cast<unsignedOutputT>((std::numeric_limits<OutputT>::max)());
}

/**
 * @brief Cast to uint, with debug-only range check, for CUDA kernel launch grid
 * or block dimensions.
 *
 * @tparam InputT The input type we intend to cast from.
 * @param[in] i Input dimension to cast.
 *
 * @return The input casted to unsigned integer.
 */
template <typename InputT>
constexpr unsigned int cuda_dim_cast(const InputT i) noexcept
{
  // On current architectures (7.5 to 12.0, both inclusive)
  // Maximum x-dimension of a grid of thread blocks: 2^31-1
  // Maximum y- or z-dimension of a grid of thread blocks: 65535
  assert(is_cast_valid<unsigned int>(i) && i < (1u << 31));

  return static_cast<unsigned int>(i);
}

/**
 * @brief Cast from an integer type to another (narrower) one, with debug-only range check.
 *
 * @tparam T The output type we intend to cast to.
 * @tparam S The input type we intend to cast from.
 * @param[in] i Input value to cast.
 *
 * @return The input casted to T.
*/
template <typename T, typename S>
constexpr NVCOMP_HOST_DEVICE_FUNCTION T narrow_cast(const S i)
{
  static_assert(
    std::numeric_limits<T>::is_integer && std::numeric_limits<S>::is_integer,
    "Types for narrow_cast must both be integers"
  );
  assert(is_cast_valid<T>(i));
  return static_cast<T>(i);
}

/**
 * @brief Return the smallest power of two larger or equal to the input x.
 *
 * @tparam T The type of the argument x.
 * @param[in] x The original amount to be rounded up.
 *
 * @return The rounded-up input.
 */
template <typename T>
constexpr NVCOMP_HOST_DEVICE_FUNCTION T roundUpPow2(const T x) noexcept
{
  size_t res = 1;
  while (res < x)
  {
    res *= 2;
  }
  return narrow_cast<T>(res);
}

} // namespace nvcomp

#endif /* DOXYGEN_SHOULD_SKIP_THIS */
