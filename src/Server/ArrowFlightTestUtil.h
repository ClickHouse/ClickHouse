#pragma once

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type_traits.h>
#include <arrow/flight/types.h>

#include <random>

namespace arrow
{
using BatchVector = std::vector<std::shared_ptr<RecordBatch>>;

class BatchIterator : public arrow::RecordBatchReader
{
public:
    BatchIterator(const std::shared_ptr<Schema> & schema, const std::vector<std::shared_ptr<RecordBatch>> & batches);

    std::shared_ptr<Schema> schema() const override;

    Status ReadNext(std::shared_ptr<RecordBatch> * out) override;

private:
    std::shared_ptr<Schema> schema_;
    std::vector<std::shared_ptr<RecordBatch>> batches_;
    size_t position_;
};

void GenerateTypedData(uint32_t * data, size_t n, uint32_t seed, uint32_t minValue, uint32_t maxValue);

void GenerateData(uint8_t * buffer, size_t n, uint32_t seed, uint32_t minValue, uint32_t maxValue);

void GenerateBitmap(uint8_t * buffer, size_t n, uint32_t seed, double null_probability, int64_t * null_count);

template <typename ArrowType>
static std::shared_ptr<NumericArray<ArrowType>>
GenerateNumericArray(int64_t size, uint32_t seed, double null_probability, uint32_t minValue, uint32_t maxValue)
{
    using CType = typename ArrowType::c_type;
    auto type = TypeTraits<ArrowType>::type_singleton();
    BufferVector buffers{2};

    int64_t null_count = 0;
    buffers[0] = *AllocateEmptyBitmap(size);
    GenerateBitmap(buffers[0]->mutable_data(), size, seed, null_probability, &null_count);

    buffers[1] = *AllocateBuffer(sizeof(CType) * size);
    GenerateData(buffers[1]->mutable_data(), size, seed, minValue, maxValue);

    auto array_data = ArrayData::Make(type, size, buffers, null_count);
    return std::make_shared<NumericArray<ArrowType>>(array_data);
}

Status GenerateAsciiString(arrow::StringBuilder * builder, size_t n, uint32_t seed);

Status MakeRandomInt32Array(int64_t length, bool include_nulls, std::shared_ptr<Array> * out, uint32_t seed);

Status MakeRandomStringArray(int64_t length, std::shared_ptr<Array> * out, uint32_t seed);

Status MakeIntBatchSized(int length, std::shared_ptr<RecordBatch> * out, uint32_t seed);

Status MakeStringBatchSized(int length, std::shared_ptr<RecordBatch> * out, uint32_t seed);

Result<BatchVector> ExampleIntBatches();

Result<BatchVector> ExampleStringBatches();

Status GetBatchForFlight(const flight::Ticket & ticket, std::shared_ptr<RecordBatchReader> * out);

std::shared_ptr<Schema> ExampleIntSchema();

std::shared_ptr<Schema> ExampleStringSchema();

Status SchemaToString(const Schema & schema, std::string * out);

arrow::Status MakeFlightInfo(
    const Schema & schema,
    const flight::FlightDescriptor & descriptor,
    const std::vector<flight::FlightEndpoint> & endpoints,
    int64_t total_records,
    int64_t total_bytes,
    flight::FlightInfo::Data * out);

std::vector<flight::FlightInfo> ExampleFlightInfo(const arrow::flight::Location & location);

}