#include <Compression/CompressionCodecDoubleDelta.h>
#include <Compression/CompressionCodecGorilla.h>

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>

#include <gtest/gtest.h>

#include <cmath>
#include <initializer_list>
#include <iomanip>
#include <memory>
#include <vector>

namespace
{
using namespace DB;

template <typename ContainerLeft, typename ContainerRight>
::testing::AssertionResult EqualContainers(const ContainerLeft & left, const ContainerRight & right)
{
    const auto MAX_MISMATCHING_ITEMS = 5;

    const auto l_size = std::size(left);
    const auto r_size = std::size(right);
    const auto size = std::min(l_size, r_size);

    ::testing::AssertionResult result = ::testing::AssertionSuccess();
    size_t mismatching_items = 0;

    if (l_size != r_size)
    {
        result = ::testing::AssertionFailure() << "size mismatch" << " expected: " << l_size << " got:" << r_size;
    }

    for (size_t i = 0; i < size; ++i)
    {
        if (left[i] != right[i])
        {
            if (result)
            {
                result = ::testing::AssertionFailure();
            }
            result << "pos " << i << ": "
                   << " expected: " << std::hex << left[i]
                   << " got:" << std::hex << right[i]
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

template <typename... Args>
std::string to_string(Args && ... args)
{
    std::ostringstream ostr;
    (ostr << ... << std::forward<Args>(args));

    return ostr.str();
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

    return CodecTestParam{std::move(data), sizeof(T), to_string(data.size(), " predefined values")};
}

template <typename T, size_t Begin = 1, size_t End = 10, typename Generator>
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
                to_string(type_name<T>(), " from ", gen_name, "(", Begin, " => ", End, ")")};
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

    ASSERT_TRUE(EqualContainers(source_data, decoded));
}

class CodecTest : public ::testing::TestWithParam<CodecTestParam>
{};

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

INSTANTIATE_TEST_CASE_P(Basic,
    CodecTest,
    ::testing::Values(
        makeParam<UInt32>(1, 2, 3, 4),
        makeParam<UInt64>(1, 2, 3, 4),
        makeParam<Float32>(1.1, 2.2, 3.3, 4.4),
        makeParam<Float64>(1.1, 2.2, 3.3, 4.4)
    )
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
    )
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
    )
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
    )
);

INSTANTIATE_TEST_CASE_P(Random,
    CodecTest,
    ::testing::Values(
        generateParam<UInt32>(G(RandomGenerator)),
        generateParam<UInt64>(G(RandomGenerator))
    )
);

INSTANTIATE_TEST_CASE_P(Overflow,
    CodecTest,
    ::testing::Values(
        generateParam<UInt32>(G(MinMaxGenerator)),
        generateParam<Int32>(G(MinMaxGenerator)),
        generateParam<UInt64>(G(MinMaxGenerator)),
        generateParam<Int64>(G(MinMaxGenerator))
    )
);

}
