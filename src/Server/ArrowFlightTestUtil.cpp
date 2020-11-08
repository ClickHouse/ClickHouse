#include "ArrowFlightTestUtil.h"

#include <Common/Exception.h>

#include <arrow/array/builder_binary.h>
#include <arrow/util/bit_util.h>

namespace DB::ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}

namespace arrow
{

BatchIterator::BatchIterator(const std::shared_ptr<Schema> & schema, const std::vector<std::shared_ptr<RecordBatch>> & batches)
    : schema_(schema), batches_(batches), position_(0)
{
}

std::shared_ptr<Schema> BatchIterator::schema() const {
    return schema_;
}

Status BatchIterator::ReadNext(std::shared_ptr<RecordBatch> * out) {
    if (position_ >= batches_.size())
        *out = nullptr;
    else
        *out = batches_[position_++];
    return Status::OK();
}

void GenerateTypedData(uint32_t * data, size_t n, uint32_t seed, uint32_t minValue, uint32_t maxValue)
{
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint32_t> dist(minValue, maxValue);

    // A static cast is required due to the int<smth> -> int8 handling.
    std::generate(data, data + n, [&dist, &rng] { return static_cast<uint32_t>(dist(rng)); });
}

void GenerateData(uint8_t * buffer, size_t n, uint32_t seed, uint32_t minValue, uint32_t maxValue)
{
    GenerateTypedData(reinterpret_cast<uint32_t *>(buffer), n, seed, minValue, maxValue);
}

void GenerateBitmap(uint8_t * buffer, size_t n, uint32_t seed, double null_probability, int64_t * null_count)
{
    int64_t count = 0;
    std::default_random_engine rng(seed);
    std::bernoulli_distribution dist(1.0 - null_probability);

    for (size_t i = 0; i < n; i++)
    {
        if (dist(rng))
            BitUtil::SetBit(buffer, i);
        else
            count++;
    }

    if (null_count != nullptr)
        *null_count = count;
}

Status GenerateAsciiString(arrow::StringBuilder * builder, size_t n, uint32_t seed) {
    static const std::string alphanum =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    std::mt19937 rng(seed);
    std::uniform_int_distribution<size_t> dist(0, alphanum.size());

    std::string result;
    for (size_t i = 0; i < n; ++i)
        result += alphanum[dist(rng)];

    return builder->Append(result);
}

Status MakeRandomInt32Array(int64_t length, bool include_nulls, std::shared_ptr<Array> * out, uint32_t seed)
{
    const double null_probability = include_nulls ? 0.5 : 0.0;

    *out = GenerateNumericArray<UInt32Type>(length, seed, null_probability, 0, 1000);

    return Status::OK();
}

Status MakeRandomStringArray(int64_t length, std::shared_ptr<Array> * out, uint32_t seed) {
    arrow::StringBuilder builder;
    for (int i = 0; i < length; ++i)
        RETURN_NOT_OK(GenerateAsciiString(&builder, i + 1, seed + i));
    return builder.Finish(out);
}

Status MakeIntBatchSized(int length, std::shared_ptr<RecordBatch> * out, uint32_t seed)
{
    // Make the schema
    auto schema = ExampleIntSchema();

    // Example data
    std::shared_ptr<Array> a0, a1;
    RETURN_NOT_OK(MakeRandomInt32Array(length, false, &a0, seed));
    RETURN_NOT_OK(MakeRandomInt32Array(length, true, &a1, seed + 1));
    *out = RecordBatch::Make(schema, length, {a0, a1});
    return Status::OK();
}

Status MakeStringBatchSized(int length, std::shared_ptr<RecordBatch> * out, uint32_t seed)
{
    // Make the schema
    auto schema = ExampleStringSchema();

    // Example data
    std::shared_ptr<Array> f0, f1;
    RETURN_NOT_OK(MakeRandomStringArray(length, &f0, seed));
    RETURN_NOT_OK(MakeRandomStringArray(length, &f1, seed + 1));
    *out = RecordBatch::Make(schema, length, {f0, f1});
    return Status::OK();
}

Status ExampleIntBatches(BatchVector * out)
{
    std::shared_ptr<RecordBatch> batch;
    for (int i = 0; i < 5; ++i)
    {
        // Make all different sizes, use different random seed
        RETURN_NOT_OK(MakeIntBatchSized(10 + i, &batch, i));
        out->push_back(batch);
    }
    return Status::OK();
}

Status ExampleStringBatches(BatchVector * out)
{
    std::shared_ptr<RecordBatch> batch;
    for (int i = 0; i < 5; ++i)
    {
        // Make all different sizes, use different random seed
        RETURN_NOT_OK(MakeStringBatchSized(10 + i, &batch, i));
        out->push_back(batch);
    }
    return Status::OK();
}

Status GetBatchForFlight(const flight::Ticket & ticket, std::shared_ptr<RecordBatchReader> * out)
{
    if (ticket.ticket == "ticket-ints-1")
    {
        BatchVector batches;
        RETURN_NOT_OK(ExampleIntBatches(&batches));
        *out = std::make_shared<BatchIterator>(batches[0]->schema(), batches);
        return Status::OK();
    }
    else if (ticket.ticket == "ticket-cmd")
    {
        BatchVector batches;
        RETURN_NOT_OK(ExampleStringBatches(&batches));
        *out = std::make_shared<BatchIterator>(batches[0]->schema(), batches);
        return Status::OK();
    }
    else
    {
        return arrow::Status::NotImplemented("no stream implemented for this ticket");
    }
}

std::shared_ptr<Schema> ExampleIntSchema()
{
    auto f0 = field("a0", int32());
    auto f1 = field("a1", int32());
    return ::arrow::schema({f0, f1});
}

std::shared_ptr<Schema> ExampleStringSchema()
{
    auto f0 = field("f0", utf8());
    auto f1 = field("f1", binary());
    return ::arrow::schema({f0, f1});
}

Status SchemaToString(const Schema& schema, std::string* out) {
    // TODO(wesm): Do we care about better memory efficiency here?
    ipc::DictionaryMemo unused_dict_memo;
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Buffer> serialized_schema,
        ipc::SerializeSchema(schema, &unused_dict_memo, default_memory_pool()));
    *out = std::string(reinterpret_cast<const char*>(serialized_schema->data()),
                       static_cast<size_t>(serialized_schema->size()));
    return Status::OK();
}

Status MakeFlightInfo(
    const Schema & schema,
    const flight::FlightDescriptor & descriptor,
    const std::vector<flight::FlightEndpoint> & endpoints,
    int64_t total_records,
    int64_t total_bytes,
    flight::FlightInfo::Data * out)
{
    out->descriptor = descriptor;
    out->endpoints = endpoints;
    out->total_records = total_records;
    out->total_bytes = total_bytes;
    return SchemaToString(schema, &out->schema);
}

#define ARROW_TEST_EXPECT_OK(expr)                                      \
  do {                                                                  \
    auto _res = (expr);                                                 \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);     \
    if (!_st.ok()) throw DB::Exception{DB::ErrorCodes::UNKNOWN_EXCEPTION, "'" ARROW_STRINGIFY(expr) "' failed with " + _st.ToString()}; \
  } while (false)

std::vector<flight::FlightInfo> ExampleFlightInfo(const arrow::flight::Location & location)
{
    flight::FlightInfo::Data flight1, flight2;

    flight::FlightEndpoint endpoint1({{"ticket-ints-1"}, {location}});
    flight::FlightEndpoint endpoint2({{"ticket-cmd"}, {location}});

    flight::FlightDescriptor descr1{flight::FlightDescriptor::PATH, "", {"ints"}};
    flight::FlightDescriptor descr2{flight::FlightDescriptor::CMD, "my_command", {}};

    auto schema1 = ExampleIntSchema();
    auto schema2 = ExampleStringSchema();

    ARROW_TEST_EXPECT_OK(MakeFlightInfo(*schema1, descr1, {endpoint1}, -1, -1, &flight1));
    ARROW_TEST_EXPECT_OK(MakeFlightInfo(*schema2, descr2, {endpoint2}, -1, -1, &flight2));
    return {flight::FlightInfo(flight1), flight::FlightInfo(flight2)};
}

#undef ARROW_TEST_EXPECT_OK

}