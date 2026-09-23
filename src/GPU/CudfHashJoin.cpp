#include <GPU/CudfHashJoin.h>

#include <GPU/Cudf.h>

#include <cudf/copying.hpp>

#include <string>

namespace DB::GPU
{

namespace
{

GPUElementType integerKeyTypeOf(GPUElementType key_element_type)
{
    if (!isInteger(key_element_type))
        throw CudfError(
            "a join key of element type " + std::to_string(static_cast<int>(key_element_type)) + " is not an integer");

    return key_element_type;
}

void checkCount(size_t actual, size_t expected, const std::string & what)
{
    if (actual != expected)
        throw CudfError(std::to_string(actual) + " " + what + ", expected " + std::to_string(expected));
}

}

CudfHashJoin::CudfHashJoin(GPUElementType key_element_type_, GPUSpan<GPUElementType> payload_element_types_)
    : key_element_type(integerKeyTypeOf(key_element_type_))
    , payload_element_types(payload_element_types_.begin(), payload_element_types_.end())
{
    initializeCudf();
}

void CudfHashJoin::build(DeviceColumnView keys, GPUSpan<DeviceColumnView> payloads)
{
    if (built)
        throw CudfError("the hash table was built twice");

    checkCount(payloads.size(), payload_element_types.size(), "payload columns of the right table");

    built = true;

    std::vector<cudf::column_view> payload_columns;
    payload_columns.reserve(payloads.size());
    for (size_t i = 0; i < payloads.size(); ++i)
    {
        checkCount(payloads[i].rows, keys.rows, "rows in payload column " + std::to_string(i));
        payload_columns.push_back(columnViewOf(payloads[i], payload_element_types[i], "payload column " + std::to_string(i)));
    }
    build_payloads = cudf::table_view(payload_columns);

    /// An empty right table needs no hash table: every probe of it matches nothing.
    if (keys.rows == 0)
        return;

    const std::vector<cudf::column_view> key_columns{columnViewOf(keys, key_element_type, "the keys of the right table")};

    hash_join = std::make_unique<cudf::hash_join>(cudf::table_view(key_columns), cudf::null_equality::EQUAL, cudfStream());
    cudfStream().synchronize();
}

size_t CudfHashJoin::probe(DeviceColumnView keys)
{
    if (!built)
        throw CudfError("a probe before the hash table was built");
    if (keys.rows == 0)
        throw CudfError("an empty block of the left table");

    gathered_payloads.reset();
    probe_indices.reset();
    probed = true;

    if (!hash_join)
        return 0;

    const rmm::cuda_stream_view stream = cudfStream();
    const std::vector<cudf::column_view> key_columns{columnViewOf(keys, key_element_type, "the keys of a left block")};

    auto [probe_side, build_side] = hash_join->inner_join(cudf::table_view(key_columns), std::nullopt, stream);

    if (probe_side->size() != build_side->size())
        throw CudfError(
            "the device returned " + std::to_string(probe_side->size()) + " probe-side indices against "
            + std::to_string(build_side->size()) + " build-side ones");

    /// The left block's own columns are indexed on the host, so only the right table's payload is
    /// gathered here.
    if (!payload_element_types.empty() && !build_side->is_empty())
    {
        const cudf::column_view build_index_column(
            cudf::data_type{cudf::type_id::INT32}, static_cast<cudf::size_type>(build_side->size()), build_side->data(), nullptr, 0);

        gathered_payloads = cudf::gather(build_payloads, build_index_column, cudf::out_of_bounds_policy::DONT_CHECK, stream);
    }

    const size_t num_matches = probe_side->size();
    probe_indices = std::move(probe_side);

    return num_matches;
}

void CudfHashJoin::copyMatchesOut(HostColumnView probe_row_indices, GPUSpan<HostColumnView> payloads)
{
    if (!probed)
        throw CudfError("a probe's result was copied out before the probe ran");

    checkCount(payloads.size(), payload_element_types.size(), "payload destinations");

    const size_t num_matches = probe_indices ? probe_indices->size() : 0;
    if (num_matches == 0)
        return;

    static_assert(sizeof(cudf::size_type) == sizeof(uint32_t), "the boundary hands back cuDF's row indices as they are");

    if (probe_row_indices.element_type != GPUElementType::UInt32)
        throw CudfError("the probe-side row indices are copied out into a column that is not UInt32");
    checkCount(probe_row_indices.rows, num_matches, "rows of room for the probe-side row indices");

    checkCuda(
        cudaMemcpyAsync(
            probe_row_indices.data, probe_indices->data(), num_matches * sizeof(cudf::size_type), cudaMemcpyDeviceToHost, cudfStream().value()),
        "cannot copy the probe-side row indices back");

    if (!payload_element_types.empty())
    {
        if (!gathered_payloads)
            throw CudfError("the device kept no gathered payload for a result of " + std::to_string(num_matches) + " rows");

        const cudf::table_view gathered = gathered_payloads->view();

        for (size_t i = 0; i < payloads.size(); ++i)
        {
            if (payloads[i].element_type != payload_element_types[i])
                throw CudfError("payload column " + std::to_string(i) + " is copied out into a column of another type");

            copyColumnToHost(gathered.column(static_cast<cudf::size_type>(i)), payloads[i], "a gathered column of the right table");
        }
    }

    cudfStream().synchronize();
}

IHashJoin * IHashJoin::create(GPUElementType key_element_type, GPUSpan<GPUElementType> payload_element_types)
{
    return new CudfHashJoin(key_element_type, payload_element_types);
}

}
