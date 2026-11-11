// `detail::vpq_train_from_rows` of cuVS' pq.cu, which `make_vpq_dataset` calls. pq.cu is not built: NVVM of nvcc 13.1
// rejects its `reconstruct_vectors_kernel` ("Explicit load/store type does not match pointee type of pointer operand").

#include "preprocessing/quantize/detail/pq.cuh"

#include <cuvs/preprocessing/quantize/pq.hpp>

#include <raft/matrix/copy.cuh>
#include <raft/util/cudart_utils.hpp>

namespace cuvs::preprocessing::quantize::pq::detail {

namespace {

template <typename T>
auto train_from_rows(raft::resources const& res,
                     cuvs::neighbors::vpq_params const& params,
                     T const* src_ptr,
                     int64_t n_rows,
                     int64_t dim,
                     int64_t stride) -> cuvs::neighbors::device_vpq_dataset<half, int64_t>
{
  cudaPointerAttributes ptr_attrs;
  RAFT_CUDA_TRY(cudaPointerGetAttributes(&ptr_attrs, src_ptr));
  auto const* device_ptr = reinterpret_cast<T const*>(ptr_attrs.devicePointer);
  if (device_ptr == nullptr) {
    RAFT_EXPECTS(stride == dim, "make_vpq_dataset: host input must be tightly packed");
    auto row_view = raft::make_host_matrix_view<const T, int64_t>(src_ptr, n_rows, dim);
    return vpq_build_half(res, params, row_view);
  }
  if (stride != dim) {
    auto dense = raft::make_device_matrix<T, int64_t>(res, n_rows, dim);
    raft::copy_matrix(
      dense.data_handle(), dim, device_ptr, stride, dim, n_rows, raft::resource::get_cuda_stream(res));
    auto dense_view =
      raft::make_device_matrix_view<const T, int64_t>(dense.data_handle(), n_rows, dim);
    return vpq_build_half(res, params, dense_view);
  }
  auto row_view = raft::make_device_matrix_view<const T, int64_t>(device_ptr, n_rows, dim);
  return vpq_build_half(res, params, row_view);
}

}  // namespace

auto vpq_train_from_rows(raft::resources const& res,
                         cuvs::neighbors::vpq_params const& params,
                         void const* src_ptr,
                         cudaDataType_t dtype,
                         int64_t n_rows,
                         int64_t dim,
                         int64_t stride) -> cuvs::neighbors::device_vpq_dataset<half, int64_t>
{
  switch (dtype) {
    case CUDA_R_32F:
      return train_from_rows(res, params, static_cast<float const*>(src_ptr), n_rows, dim, stride);
    case CUDA_R_16F:
      return train_from_rows(res, params, static_cast<half const*>(src_ptr), n_rows, dim, stride);
    case CUDA_R_8I:
      return train_from_rows(res, params, static_cast<int8_t const*>(src_ptr), n_rows, dim, stride);
    case CUDA_R_8U:
      return train_from_rows(res, params, static_cast<uint8_t const*>(src_ptr), n_rows, dim, stride);
    default:
      RAFT_FAIL("make_vpq_dataset: unsupported dataset element type %d", static_cast<int>(dtype));
  }
}

}  // namespace cuvs::preprocessing::quantize::pq::detail
