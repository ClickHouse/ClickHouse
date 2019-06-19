#include <Compression/CompressionCodecDoubleDelta.h>
#include <Compression/CompressionCodecGorilla.h>

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/PODArray.h>

#include <boost/format.hpp>

#include <cmath>
#include <initializer_list>
#include <iomanip>
#include <memory>
#include <vector>
#include <typeinfo>
#include <iterator>
#include <optional>
#include <iostream>
#include <bitset>
#include <string.h>

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wundef"
#endif
#include <gtest/gtest.h>

using namespace DB;

template <typename T>
std::string bin(const T & value, size_t bits = sizeof(T)*8)
{
    static const UInt8 MAX_BITS = sizeof(T)*8;
    assert(bits <= MAX_BITS);

    return std::bitset<sizeof(T) * 8>(static_cast<unsigned long long>(value))
            .to_string().substr(MAX_BITS - bits, bits);
}

template <typename T>
const char* type_name()
{
    return typeid(T).name();
}

template <>
const char* type_name<UInt32>()
{
    return "uint32";
}

template <>
const char* type_name<Int32>()
{
    return "int32";
}

template <>
const char* type_name<UInt64>()
{
    return "uint64";
}

template <>
const char* type_name<Int64>()
{
    return "int64";
}

template <>
const char* type_name<Float32>()
{
    return "float";
}

template <>
const char* type_name<Float64>()
{
    return "double";
}


template <typename T, typename ContainerLeft, typename ContainerRight>
::testing::AssertionResult EqualByteContainersAs(const ContainerLeft & left, const ContainerRight & right)
{
    static_assert(sizeof(typename ContainerLeft::value_type) == 1, "Expected byte-container");
    static_assert(sizeof(typename ContainerRight::value_type) == 1, "Expected byte-container");

    ::testing::AssertionResult result = ::testing::AssertionSuccess();

    ReadBufferFromMemory left_read_buffer(left.data(), left.size());
    ReadBufferFromMemory right_read_buffer(right.data(), right.size());

    const auto l_size = left.size() / sizeof(T);
    const auto r_size = right.size() / sizeof(T);
    const auto size = std::min(l_size, r_size);

    if (l_size != r_size)
    {
        result = ::testing::AssertionFailure() << "size mismatch" << " expected: " << l_size << " got:" << r_size;
    }

    const auto MAX_MISMATCHING_ITEMS = 5;
    int mismatching_items = 0;
    for (int i = 0; i < size; ++i)
    {
        T left_value{};
        left_read_buffer.readStrict(reinterpret_cast<char*>(&left_value), sizeof(left_value));

        T right_value{};
        right_read_buffer.readStrict(reinterpret_cast<char*>(&right_value), sizeof(right_value));

        if (left_value != right_value)
        {
            if (result)
            {
                result = ::testing::AssertionFailure();
            }

            result << "mismatching " << sizeof(T) << "-byte item #" << i
                   << "\nexpected: " << bin(left_value)
                   << "\ngot     : " << bin(right_value)
                   << std::endl;

            if (++mismatching_items >= MAX_MISMATCHING_ITEMS)
            {
                result << "..." << std::endl;
                break;
            }
        }
    }

    return result;
}

struct CodecTestParam
{
    std::vector<char> source_data;
    UInt8 data_byte_size;
    std::string case_name;
};

std::ostream & operator<<(std::ostream & ostr, const CodecTestParam & param)
{
    return ostr << "name: " << param.case_name
                << "\nbyte size: " << static_cast<UInt32>(param.data_byte_size)
                << "\ndata size: " << param.source_data.size();
}

template <typename T, typename... Args>
CodecTestParam makeParam(Args && ... args)
{
    std::initializer_list<T> vals{static_cast<T>(args)...};
    std::vector<char> data(sizeof(T) * std::size(vals));

    char * write_pos = data.data();
    for (const auto & v : vals)
    {
        unalignedStore<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestParam{std::move(data), sizeof(T),
                (boost::format("%1% %2%") % data.size() % " predefined values").str()};
}

template <typename T, size_t Begin = 1, size_t End = 10000, typename Generator>
CodecTestParam generateParam(Generator gen, const char* gen_name)
{
    static_assert (End >= Begin, "End must be not less than Begin");

    std::vector<char> data(sizeof(T) * (End - Begin));
    char * write_pos = data.data();

    for (size_t i = Begin; i < End; ++i)
    {
        const T v = gen(static_cast<T>(i));
        unalignedStore<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestParam{std::move(data), sizeof(T),
                (boost::format("%1% from %2% (%3% => %4%)") % type_name<T>() % gen_name % Begin % End).str()};
}

void TestTranscoding(ICompressionCodec * codec, const CodecTestParam & param)
{
    const auto & source_data = param.source_data;

    const UInt32 encoded_max_size = codec->getCompressedReserveSize(source_data.size());
    PODArray<char> encoded(encoded_max_size);

    const UInt32 encoded_size = codec->compress(source_data.data(), source_data.size(), encoded.data());
    encoded.resize(encoded_size);

    PODArray<char> decoded(source_data.size());
    const UInt32 decoded_size = codec->decompress(encoded.data(), encoded.size(), decoded.data());
    decoded.resize(decoded_size);

    switch (param.data_byte_size)
    {
        case 1:
            ASSERT_TRUE(EqualByteContainersAs<UInt8>(source_data, decoded));
            break;
        case 2:
            ASSERT_TRUE(EqualByteContainersAs<UInt16>(source_data, decoded));
            break;
        case 4:
            ASSERT_TRUE(EqualByteContainersAs<UInt32>(source_data, decoded));
            break;
        case 8:
            ASSERT_TRUE(EqualByteContainersAs<UInt64>(source_data, decoded));
            break;
        default:
            FAIL() << "Invalid data_byte_size: " << param.data_byte_size;
    }
}

class CodecTest : public ::testing::TestWithParam<CodecTestParam>
{
public:
    static void SetUpTestCase()
    {
        // To make random predicatble and avoid failing test "out of the blue".
        srand(0);
    }
};

TEST_P(CodecTest, DoubleDelta)
{
    const auto & param = GetParam();
    auto codec = std::make_unique<CompressionCodecDoubleDelta>(param.data_byte_size);

    TestTranscoding(codec.get(), param);
}

TEST_P(CodecTest, Gorilla)
{
    const auto & param = GetParam();
    auto codec = std::make_unique<CompressionCodecGorilla>(param.data_byte_size);

    TestTranscoding(codec.get(), param);
}

auto SameValueGenerator = [](auto value)
{
    return [=](auto i)
    {
        return static_cast<decltype(i)>(value);
    };
};

auto SequentialGenerator = [](auto stride = 1)
{
    return [=](auto i)
    {
        using ValueType = decltype(i);
        return static_cast<ValueType>(stride * i);
    };
};

template <typename T>
struct MonotonicGenerator
{
    MonotonicGenerator(T stride = 1, size_t max_step = 10)
        : prev_value{},
          stride(stride),
          max_step(max_step)
    {}

    template <typename U>
    U operator()(U i)
    {
        if (!prev_value.has_value())
        {
            prev_value = i * stride;
        }

        const U result = *prev_value + static_cast<T>(stride * (rand() % max_step));

        prev_value = result;
        return result;
    }

    std::optional<T> prev_value;
    const T stride;
    const size_t max_step;
};

auto MinMaxGenerator = [](auto i)
{
    if (i % 2 == 0)
    {
        return std::numeric_limits<decltype(i)>::min();
    }
    else
    {
        return std::numeric_limits<decltype(i)>::max();
    }
};

auto RandomGenerator = [](auto i) {return static_cast<decltype(i)>(rand());};

auto RandomishGenerator = [](auto i)
{
    return static_cast<decltype(i)>(sin(static_cast<double>(i) * i) * i);
};

INSTANTIATE_TEST_CASE_P(Basic,
    CodecTest,
    ::testing::Values(
        makeParam<UInt32>(1, 2, 3, 4),
        makeParam<UInt64>(1, 2, 3, 4),
        makeParam<Float32>(1.1, 2.2, 3.3, 4.4),
        makeParam<Float64>(1.1, 2.2, 3.3, 4.4)
    ),
);

#define G(generator) generator, #generator

INSTANTIATE_TEST_CASE_P(Same,
    CodecTest,
    ::testing::Values(
        generateParam<UInt32>(G(SameValueGenerator(1000))),
        generateParam<Int32>(G(SameValueGenerator(-1000))),
        generateParam<UInt64>(G(SameValueGenerator(1000))),
        generateParam<Int64>(G(SameValueGenerator(-1000))),
        generateParam<Float32>(G(SameValueGenerator(M_E))),
        generateParam<Float64>(G(SameValueGenerator(M_E)))
    ),
);

INSTANTIATE_TEST_CASE_P(Sequential,
    CodecTest,
    ::testing::Values(
        generateParam<UInt32>(G(SequentialGenerator(1))),
        generateParam<Int32>(G(SequentialGenerator(-1))),
        generateParam<UInt64>(G(SequentialGenerator(1))),
        generateParam<Int64>(G(SequentialGenerator(-1))),
        generateParam<Float32>(G(SequentialGenerator(M_E))),
        generateParam<Float64>(G(SequentialGenerator(M_E)))
    ),
);

INSTANTIATE_TEST_CASE_P(Monotonic,
    CodecTest,
    ::testing::Values(
        generateParam<UInt32>(G(MonotonicGenerator<UInt32>(1, 5))),
        generateParam<Int32>(G(MonotonicGenerator<Int32>(-1, 5))),
        generateParam<UInt64>(G(MonotonicGenerator<UInt64>(1, 5))),
        generateParam<Int64>(G(MonotonicGenerator<Int64>(-1, 5))),
        generateParam<Float32>(G(MonotonicGenerator<Float32>(M_E, 5))),
        generateParam<Float64>(G(MonotonicGenerator<Float64>(M_E, 5)))
    ),
);

INSTANTIATE_TEST_CASE_P(Random,
    CodecTest,
    ::testing::Values(
        generateParam<UInt32>(G(RandomGenerator)),
        generateParam<UInt64>(G(RandomGenerator))
    ),
);

INSTANTIATE_TEST_CASE_P(RandomLike,
    CodecTest,
    ::testing::Values(
        generateParam<Int32>(G(RandomishGenerator)),
        generateParam<Int64>(G(RandomishGenerator)),
        generateParam<Float32>(G(RandomishGenerator)),
        generateParam<Float64>(G(RandomishGenerator))
    ),
);

INSTANTIATE_TEST_CASE_P(Overflow,
    CodecTest,
    ::testing::Values(
        generateParam<UInt32>(G(MinMaxGenerator)),
        generateParam<Int32>(G(MinMaxGenerator)),
        generateParam<UInt64>(G(MinMaxGenerator)),
        generateParam<Int64>(G(MinMaxGenerator))
    ),
);
