# Keep KMeans center scalar quantization finite when a dimension's maximum
# absolute value is too small for its `int8_t` multiplier to fit in `float`.
set(_scalar_quantization_src "${SCANN_SOURCE_DIR}/scann/utils/scalar_quantization_helpers.cc")
set(_scalar_quantization_dst "${CMAKE_CURRENT_BINARY_DIR}/scalar_quantization_helpers.cc")
configure_file("${_scalar_quantization_src}" "${_scalar_quantization_dst}" COPYONLY)
file(READ "${_scalar_quantization_dst}" _scalar_quantization_content)
scann_checked_replace(
[==[  for (float& f : multipliers) {
    if (f == 0.0f) {
      f = 1.0f;
    } else {
      f = numeric_limits<int8_t>::max() / f;
    }
  }]==]
[==[  for (float& f : multipliers) {
    f = numeric_limits<int8_t>::max() / f;
    if (f == std::numeric_limits<float>::infinity()) {
      f = 1.0f;
    }
  }]==]
    _scalar_quantization_content "${_scalar_quantization_content}")
file(WRITE "${_scalar_quantization_dst}" "${_scalar_quantization_content}")
list(REMOVE_ITEM SCANN_SOURCES "${_scalar_quantization_src}")
list(APPEND SCANN_SOURCES "${_scalar_quantization_dst}")
