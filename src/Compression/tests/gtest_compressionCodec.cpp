#include "config.h"

#include <Compression/CompressionFactory.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromMemory.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>

#include <Compression/ICompressionCodec.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Compression/getCompressionCodecForFile.h>
#include <IO/BufferWithOwnMemory.h>

#include <random>
#include <bitset>
#include <cmath>
#include <initializer_list>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <numbers>
#include <typeinfo>
#include <vector>

#include <cstring>

/// For the expansion of gtest macros.
#include <gtest/gtest.h>

#if USE_SZ3
#    include <SZ3/api/sz.hpp>
#    include <SZ3/lossless/Lossless_zstd.hpp>
#    include <zstd.h>
#endif

using namespace DB;

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int TOO_LARGE_SIZE_COMPRESSED;
}

namespace
{

template <class T> inline constexpr bool is_pod_v = std::is_trivial_v<std::is_standard_layout<T>>;

template <typename T>
struct AsHexStringHelper
{
    const T & container;
};

template <typename T>
std::ostream & operator << (std::ostream & ostr, const AsHexStringHelper<T> & helper)
{
    ostr << std::hex;
    for (const auto & e : helper.container)
    {
        ostr << "\\x" << std::setw(2) << std::setfill('0') << (static_cast<unsigned int>(e) & 0xFF);
    }

    return ostr;
}

template <typename T>
AsHexStringHelper<T> AsHexString(const T & container)
{
    static_assert (sizeof(container[0]) == 1 && is_pod_v<std::decay_t<decltype(container[0])>>, "Only works on containers of byte-size PODs.");

    return AsHexStringHelper<T>{container};
}

template <typename T>
std::string bin(const T & value, size_t bits = sizeof(T)*8)
{
    static const uint8_t MAX_BITS = sizeof(T)*8;
    chassert(bits <= MAX_BITS);

    return std::bitset<sizeof(T) * 8>(static_cast<uint64_t>(value))
            .to_string().substr(MAX_BITS - bits, bits);
}

template <typename T>
const char* type_name()
{
#define MAKE_TYPE_NAME(TYPE) \
    if constexpr (std::is_same_v<TYPE, T>) return #TYPE

    MAKE_TYPE_NAME(UInt8);
    MAKE_TYPE_NAME(UInt16);
    MAKE_TYPE_NAME(UInt32);
    MAKE_TYPE_NAME(UInt64);
    MAKE_TYPE_NAME(Int8);
    MAKE_TYPE_NAME(Int16);
    MAKE_TYPE_NAME(Int32);
    MAKE_TYPE_NAME(Int64);
    MAKE_TYPE_NAME(Float32);
    MAKE_TYPE_NAME(Float64);

#undef MAKE_TYPE_NAME

    return typeid(T).name();
}

template <typename T>
DataTypePtr makeDataType()
{
#define MAKE_DATA_TYPE(TYPE) \
    if constexpr (std::is_same_v<T, TYPE>) return std::make_shared<DataType ## TYPE>()

    MAKE_DATA_TYPE(UInt8);
    MAKE_DATA_TYPE(UInt16);
    MAKE_DATA_TYPE(UInt32);
    MAKE_DATA_TYPE(UInt64);
    MAKE_DATA_TYPE(Int8);
    MAKE_DATA_TYPE(Int16);
    MAKE_DATA_TYPE(Int32);
    MAKE_DATA_TYPE(Int64);
    MAKE_DATA_TYPE(Float32);
    MAKE_DATA_TYPE(Float64);

#undef MAKE_DATA_TYPE

    chassert(false && "unknown datatype");
    return nullptr;
}

template <typename T, typename Container>
class BinaryDataAsSequenceOfValuesIterator
{
    const Container & container;
    const void * data;
    const void * data_end;

    T current_value;

public:
    using Self = BinaryDataAsSequenceOfValuesIterator<T, Container>;

    explicit BinaryDataAsSequenceOfValuesIterator(const Container & container_)
        : container(container_),
          data(container.data()),
          data_end(container.data() + container.size()),
          current_value(T{})
    {
        static_assert(sizeof(container[0]) == 1 && is_pod_v<std::decay_t<decltype(container[0])>>, "Only works on containers of byte-size PODs.");
        read();
    }

    const T & operator*() const
    {
        return current_value;
    }

    size_t itemsLeft() const
    {
        return reinterpret_cast<const char *>(data_end) - reinterpret_cast<const char *>(data);
    }

    Self & operator++()
    {
        read();
        return *this;
    }

    explicit operator bool() const
    {
        return itemsLeft() > 0;
    }

private:
    void read()
    {
        if (!*this)
        {
            throw std::runtime_error("No more data to read");
        }

        current_value = unalignedLoadLittleEndian<T>(data);
        data = reinterpret_cast<const char *>(data) + sizeof(T);
    }
};

template <typename T, typename Container>
BinaryDataAsSequenceOfValuesIterator<T, Container> AsSequenceOf(const Container & container)
{
    return BinaryDataAsSequenceOfValuesIterator<T, Container>(container);
}

template <typename T, typename ContainerLeft, typename ContainerRight>
::testing::AssertionResult EqualByteContainersAs(const ContainerLeft & left, const ContainerRight & right)
{
    static_assert(sizeof(typename ContainerLeft::value_type) == 1, "Expected byte-container");
    static_assert(sizeof(typename ContainerRight::value_type) == 1, "Expected byte-container");

    ::testing::AssertionResult result = ::testing::AssertionSuccess();

    const auto l_size = left.size() / sizeof(T);
    const auto r_size = right.size() / sizeof(T);
    const auto size = std::min(l_size, r_size);

    if (l_size != r_size)
    {
        result = ::testing::AssertionFailure() << "size mismatch, expected: " << l_size << " got:" << r_size;
    }
    if (l_size == 0 || r_size == 0)
    {
        return result;
    }

    auto l = AsSequenceOf<T>(left);
    auto r = AsSequenceOf<T>(right);

    static constexpr auto MAX_MISMATCHING_ITEMS = 5;
    int mismatching_items = 0;
    size_t i = 0;

    while (l && r)
    {
        const auto left_value = *l;
        const auto right_value = *r;
        ++l;
        ++r;
        ++i;

        if (left_value != right_value)
        {
            if (result)
            {
                result = ::testing::AssertionFailure();
            }

            if (++mismatching_items <= MAX_MISMATCHING_ITEMS)
            {
                result << "\nmismatching " << sizeof(T) << "-byte item #" << i
                   << "\nexpected: " << bin(left_value) << " (0x" << std::hex << size_t(left_value) << ")"
                   << "\ngot     : " << bin(right_value) << " (0x" << std::hex << size_t(right_value) << ")";
                if (mismatching_items == MAX_MISMATCHING_ITEMS)
                {
                    result << "\n..." << std::endl;
                }
            }
        }
    }
    if (mismatching_items > 0)
    {
        result << "total mismatching items:" << mismatching_items << " of " << size;
    }

    return result;
}

template <typename ContainerLeft, typename ContainerRight>
::testing::AssertionResult EqualByteContainers(uint8_t element_size, const ContainerLeft & left, const ContainerRight & right)
{
    switch (element_size)
    {
        case 1:
            return EqualByteContainersAs<UInt8>(left, right);
            break;
        case 2:
            return EqualByteContainersAs<UInt16>(left, right);
            break;
        case 4:
            return EqualByteContainersAs<UInt32>(left, right);
            break;
        case 8:
            return EqualByteContainersAs<UInt64>(left, right);
            break;
        default:
            chassert(false && "Invalid element_size");
            return ::testing::AssertionFailure() << "Invalid element_size: " << element_size;
    }
}

struct Codec
{
    std::string codec_statement;
    std::optional<double> expected_compression_ratio;

    explicit Codec(std::string codec_statement_, std::optional<double> expected_compression_ratio_ = std::nullopt)
        : codec_statement(std::move(codec_statement_)),
          expected_compression_ratio(expected_compression_ratio_)
    {}
};


struct CodecTestSequence
{
    std::string name;
    std::vector<char> serialized_data;
    DataTypePtr data_type;

    CodecTestSequence(std::string name_, std::vector<char> serialized_data_, DataTypePtr data_type_)
        : name(name_),
          serialized_data(serialized_data_),
          data_type(data_type_)
    {}

    CodecTestSequence & append(const CodecTestSequence & other)
    {
        chassert(data_type->equals(*other.data_type));

        serialized_data.insert(serialized_data.end(), other.serialized_data.begin(), other.serialized_data.end());
        if (!name.empty())
            name += " + ";
        name += other.name;

        return *this;
    }
};

CodecTestSequence operator+(CodecTestSequence && left, const CodecTestSequence & right)
{
    return left.append(right);
}

std::vector<CodecTestSequence> operator+(const std::vector<CodecTestSequence> & left, const std::vector<CodecTestSequence> & right)
{
    std::vector<CodecTestSequence> result(left);
    std::move(std::begin(right), std::end(right), std::back_inserter(result));

    return result;
}

template <typename T>
CodecTestSequence operator*(CodecTestSequence && left, T times)
{
    std::vector<char> data(std::move(left.serialized_data));
    const size_t initial_size = data.size();
    const size_t final_size = initial_size * times;

    data.reserve(final_size);

    for (T i = 0; i < times; ++i)
    {
        data.insert(data.end(), data.begin(), data.begin() + initial_size);
    }

    return CodecTestSequence{
        left.name + " x " + std::to_string(times),
        std::move(data),
        std::move(left.data_type)
    };
}

std::ostream & operator<<(std::ostream & ostr, const Codec & codec)
{
    ostr << "Codec{"
         << "name: " << codec.codec_statement;
    if (codec.expected_compression_ratio)
        return ostr << ", expected_compression_ratio: " << *codec.expected_compression_ratio << "}";
    else
        return ostr << "}";
}

std::ostream & operator<<(std::ostream & ostr, const CodecTestSequence & seq)
{
    return ostr << "CodecTestSequence{"
                << "name: " << seq.name
                << ", type name: " << seq.data_type->getName()
                << ", data size: " << seq.serialized_data.size() << " bytes"
                << "}";
}

template <typename T, typename... Args>
CodecTestSequence makeSeq(Args && ... args)
{
    std::initializer_list<T> vals{static_cast<T>(args)...};
    std::vector<char> data(sizeof(T) * std::size(vals));

    char * write_pos = data.data();
    for (const auto & v : vals)
    {
        unalignedStoreLittleEndian<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestSequence{
            (fmt::format("{} values of {}", std::size(vals), type_name<T>())),
            std::move(data),
            makeDataType<T>()
    };
}

template <typename T, typename Generator, typename B = int, typename E = int>
CodecTestSequence generateSeq(Generator gen, const char* gen_name, B Begin = 0, E End = 10000)
{
    const auto direction = std::signbit(End - Begin) ? -1 : 1;
    std::vector<char> data(sizeof(T) * (End - Begin));
    char * write_pos = data.data();

    for (auto i = Begin; i < End; i += direction)
    {
        /// Pass index as T so generators using decltype(i) produce values of the target type.
        const T v = static_cast<T>(gen(static_cast<T>(i)));

        unalignedStoreLittleEndian<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestSequence{
            (fmt::format("{} values of {} from {}", (End - Begin), type_name<T>(), gen_name)),
            std::move(data),
            makeDataType<T>()
    };
}

struct NoOpTimer
{
    void start() {}
    void report(const char*) {}
};

struct StopwatchTimer
{
    explicit StopwatchTimer(clockid_t clock_type, size_t estimated_marks = 32)
        : stopwatch(clock_type)
    {
        results.reserve(estimated_marks);
    }

    void start()
    {
        stopwatch.restart();
    }

    void report(const char * mark)
    {
        results.emplace_back(mark, stopwatch.elapsed());
    }

    void stop()
    {
        stopwatch.stop();
    }

    const std::vector<std::tuple<const char*, UInt64>> & getResults() const
    {
        return results;
    }

private:
    Stopwatch stopwatch;
    std::vector<std::tuple<const char*, UInt64>> results;
};

CompressionCodecPtr makeCodec(const std::string & codec_string, const DataTypePtr data_type)
{
    const std::string codec_statement = "(" + codec_string + ")";
    Tokens tokens(codec_statement.begin().base(), codec_statement.end().base());
    IParser::Pos token_iterator(tokens, 0, 0);

    Expected expected;
    ASTPtr codec_ast;
    ParserCodec parser;

    parser.parse(token_iterator, codec_ast, expected);

    return CompressionCodecFactory::instance().get(codec_ast, data_type);
}

template <typename Timer>
void testTranscoding(Timer & timer, ICompressionCodec & codec, const CodecTestSequence & test_sequence,
                     std::optional<double> expected_compression_ratio = {})
{
    const auto & source_data = test_sequence.serialized_data;

    const UInt32 encoded_max_size = codec.getCompressedReserveSize(
        static_cast<UInt32>(source_data.size()));
    PODArray<char> encoded(encoded_max_size);

    timer.start();

    chassert(source_data.data() != nullptr); // Codec assumes that source buffer is not null.
    const UInt32 encoded_size = codec.compress(
        source_data.data(), static_cast<UInt32>(source_data.size()), encoded.data());
    timer.report("encoding");

    encoded.resize(encoded_size);

    PODArray<char> decoded(source_data.size());

    timer.start();
    const UInt32 decoded_size = codec.decompress(
        encoded.data(), static_cast<UInt32>(encoded.size()), decoded.data());
    timer.report("decoding");

    decoded.resize(decoded_size);

    ASSERT_TRUE(EqualByteContainers(static_cast<uint8_t>(test_sequence.data_type->getSizeOfValueInMemory()), source_data, decoded));

    const auto header_size = ICompressionCodec::getHeaderSize();
    const auto compression_ratio = (encoded_size - header_size) / static_cast<double>(source_data.size());

    if (expected_compression_ratio)
    {
        ASSERT_LE(compression_ratio, *expected_compression_ratio)
                << "\n\tdecoded size: " << source_data.size()
                << "\n\tencoded size: " << encoded_size
                << "(no header: " << encoded_size - header_size << ")";
    }
}

class CodecTest : public ::testing::TestWithParam<std::tuple<Codec, CodecTestSequence>>
{
public:
    enum MakeCodecParam
    {
        CODEC_WITH_DATA_TYPE,
        CODEC_WITHOUT_DATA_TYPE,
    };

    static CompressionCodecPtr makeCodec(MakeCodecParam with_data_type)
    {
        const auto & codec_string = std::get<0>(GetParam()).codec_statement;
        const auto & data_type = with_data_type == CODEC_WITH_DATA_TYPE ? std::get<1>(GetParam()).data_type : nullptr;

        return ::makeCodec(codec_string, data_type);
    }

    static void testTranscoding(ICompressionCodec & codec)
    {
        NoOpTimer timer;
        ::testTranscoding(timer, codec, std::get<1>(GetParam()), std::get<0>(GetParam()).expected_compression_ratio);
    }
};

TEST_P(CodecTest, TranscodingWithDataType)
{
    /// Gorilla and ALP can only be applied to floating point columns
    const auto & codec_statement = std::get<0>(GetParam()).codec_statement;
    const bool codec_is_float_point = codec_statement.contains("Gorilla") || codec_statement.contains("ALP");
    const WhichDataType which(std::get<1>(GetParam()).data_type.get());
    const bool data_is_float = which.isFloat();
    if (codec_is_float_point && !data_is_float)
        GTEST_SKIP() << "Skipping Float-point-compressed non-float column";

    const auto codec = makeCodec(CODEC_WITH_DATA_TYPE);
    testTranscoding(*codec);
}


// Param is tuple-of-tuple to simplify instantiating with values, since typically group of cases test only one codec.
class CodecTestCompatibility : public ::testing::TestWithParam<std::tuple<Codec, std::tuple<CodecTestSequence, std::string>>>
{};

// Check that input sequence when encoded matches the encoded string binary.
TEST_P(CodecTestCompatibility, Encoding)
{
    const auto & codec_spec = std::get<0>(GetParam());
    const auto & [data_sequence, expected] = std::get<1>(GetParam());
    const auto codec = makeCodec(codec_spec.codec_statement, data_sequence.data_type);

    const auto & source_data = data_sequence.serialized_data;

    // Just encode the data with codec
    const UInt32 encoded_max_size = codec->getCompressedReserveSize(
        static_cast<UInt32>(source_data.size()));
    PODArray<char> encoded(encoded_max_size);

    const UInt32 encoded_size = codec->compress(
        source_data.data(), static_cast<UInt32>(source_data.size()), encoded.data());
    encoded.resize(encoded_size);
    SCOPED_TRACE(::testing::Message("encoded:  ") << AsHexString(encoded));

    ASSERT_TRUE(EqualByteContainersAs<UInt8>(expected, encoded));
}

// Check that binary string is exactly decoded into input sequence.
TEST_P(CodecTestCompatibility, Decoding)
{
    const auto & codec_spec = std::get<0>(GetParam());
    const auto & [expected, encoded_data] = std::get<1>(GetParam());
    const auto codec = makeCodec(codec_spec.codec_statement, expected.data_type);

    PODArray<char> decoded(expected.serialized_data.size());
    const UInt32 decoded_size = codec->decompress(
        encoded_data.c_str(), static_cast<UInt32>(encoded_data.size()), decoded.data());
    decoded.resize(decoded_size);

    ASSERT_TRUE(EqualByteContainers(static_cast<UInt8>(expected.data_type->getSizeOfValueInMemory()), expected.serialized_data, decoded));
}

class CodecTestPerformance : public ::testing::TestWithParam<std::tuple<Codec, CodecTestSequence>>
{};

TEST_P(CodecTestPerformance, TranscodingWithDataType)
{
    const auto & [codec_spec, test_seq] = GetParam();
    const auto codec = ::makeCodec(codec_spec.codec_statement, test_seq.data_type);

    const auto runs = 10;
    std::map<std::string, std::vector<UInt64>> results;

    for (size_t i = 0; i < runs; ++i)
    {
        StopwatchTimer timer{CLOCK_THREAD_CPUTIME_ID};
        ::testTranscoding(timer, *codec, test_seq);
        timer.stop();

        for (const auto & [label, value] : timer.getResults())
        {
            results[label].push_back(value);
        }
    }

    auto compute_mean_and_stddev = [](const auto & values)
    {
        double mean{};

        if (values.size() < 2)
            return std::make_tuple(mean, double{});

        using ValueType = typename std::decay_t<decltype(values)>::value_type;
        std::vector<ValueType> tmp_v(std::begin(values), std::end(values));
        std::sort(tmp_v.begin(), tmp_v.end());

        // remove min and max
        tmp_v.erase(tmp_v.begin());
        tmp_v.erase(tmp_v.end() - 1);

        for (const auto & v : tmp_v)
        {
            mean += static_cast<double>(v);
        }

        mean = mean / static_cast<double>(tmp_v.size());
        double std_dev = 0.0;
        for (const auto & v : tmp_v)
        {
            const auto d = (static_cast<double>(v) - mean);
            std_dev += (d * d);
        }
        std_dev = std::sqrt(std_dev / static_cast<double>(tmp_v.size()));

        return std::make_tuple(mean, std_dev);
    };

    std::cerr << codec_spec.codec_statement
              << " " << test_seq.data_type->getName()
              << " (" << test_seq.serialized_data.size() << " bytes, "
              << std::hex << CityHash_v1_0_2::CityHash64(test_seq.serialized_data.data(), test_seq.serialized_data.size()) << std::dec
              << ", average of " << runs << " runs, μs)";

    for (const auto & k : {"encoding", "decoding"})
    {
        const auto & values = results[k];
        const auto & [mean, std_dev] = compute_mean_and_stddev(values);
        // Ensure that Coefficient of variation is reasonably low, otherwise these numbers are meaningless
        EXPECT_GT(0.05, std_dev / mean);
        std::cerr << "\t" << std::fixed << std::setprecision(1) << mean / 1000.0;
    }

    std::cerr << std::endl;
}
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(CodecTestPerformance);

///////////////////////////////////////////////////////////////////////////////////////////////////
// Here we use generators to produce test payload for codecs.
// Generator is a callable that can produce infinite number of values,
// output value MUST be of the same type as input value.
///////////////////////////////////////////////////////////////////////////////////////////////////

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
        return static_cast<ValueType>(static_cast<ValueType>(stride) * i);
    };
};

// Generator that helps debugging output of other generators
// by logging every output value alongside iteration index and input.
//auto LoggingProxyGenerator = [](auto other_generator, const char * name, std::ostream & ostr, const int limit = std::numeric_limits<int>::max())
//{
//    ostr << "\n\nValues from " << name << ":\n";
//    auto count = std::make_shared<int>(0);
//    return [&, count](auto i)
//    {
//        using ValueType = decltype(i);
//        const auto ret = static_cast<ValueType>(other_generator(i));
//        if (++(*count) < limit)
//        {
//            ostr << "\t" << *count << " : " << i << " => " << ret << "\n";
//        }

//        return ret;
//    };
//};

template <typename T>
using uniform_distribution =
typename std::conditional_t<std::is_floating_point_v<T>, std::uniform_real_distribution<T>,
        typename std::conditional_t<is_integer<T>, std::uniform_int_distribution<T>, void>>;


template <typename T = Int32>
struct MonotonicGenerator // NOLINT
{
    explicit MonotonicGenerator(T stride_ = 1, T max_step = 10) // NOLINT
        : prev_value(0),
          stride(stride_),
          random_engine(0), /// NOLINT
          distribution(0, max_step)
    {}

    template <typename U>
    U operator()(U)
    {
        prev_value = prev_value + stride * distribution(random_engine);
        return static_cast<U>(prev_value);
    }

private:
    T prev_value;
    const T stride;
    std::default_random_engine random_engine;
    uniform_distribution<T> distribution;
};

template <typename T>
struct RandomGenerator
{
    explicit RandomGenerator(T seed = 0, T value_min = std::numeric_limits<T>::min(), T value_max = std::numeric_limits<T>::max())
        : random_engine(static_cast<uint_fast32_t>(seed)),
          distribution(value_min, value_max)
    {
    }

    template <typename U>
    U operator()(U)
    {
        return static_cast<U>(distribution(random_engine));
    }

private:
    std::default_random_engine random_engine;
    uniform_distribution<T> distribution;
};

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"
MonotonicGenerator() -> MonotonicGenerator<Int32>;
#pragma clang diagnostic pop

auto RandomishGenerator = [](auto i)
{
    using T = decltype(i);
    double sin_value = sin(static_cast<double>(i * i)) * static_cast<double>(i);
    if (sin_value < static_cast<double>(std::numeric_limits<T>::lowest()) || sin_value > static_cast<double>(std::numeric_limits<T>::max()))
        return T{};
    return T(sin_value);
};

auto MinMaxGenerator = []()
{
    return [step = 0](auto i) mutable
    {
        if (step++ % 2 == 0)
        {
            return std::numeric_limits<decltype(i)>::min();
        }
        else
        {
            return std::numeric_limits<decltype(i)>::max();
        }
    };
};

// Fill dest value with 0x00 or 0xFF
auto FFand0Generator = []()
{
    return [step = 0](auto i) mutable
    {
        decltype(i) result;

        if (step % 2 == 0)
            memset(&result, 0, sizeof(result));
        else
            memset(&result, 0xFF, sizeof(result));

        ++step;
        return result;
    };
};


// Makes many sequences with generator, first sequence length is 0, second is 1..., third is 2 up to `sequences_count`.
template <typename T, typename Generator>
std::vector<CodecTestSequence> generatePyramidOfSequences(const size_t sequences_count, Generator && generator, const char* generator_name)
{
    std::vector<CodecTestSequence> sequences;
    sequences.reserve(sequences_count);

    // Don't test against sequence of size 0, since it causes a nullptr source buffer as codec input and produces an error.
    // sequences.push_back(makeSeq<T>()); // sequence of size 0
    for (size_t i = 1; i < sequences_count; ++i)
    {
        std::string name = generator_name + std::string(" from 0 to ") + std::to_string(i);
        sequences.push_back(generateSeq<T>(generator, name.c_str(), 0, i));
    }

    return sequences;
}

// helper macro to produce human-friendly sequence name from generator
#define G(generator) generator, #generator

const auto DefaultCodecsToTest = ::testing::Values(
    Codec("DoubleDelta"),
    Codec("DoubleDelta, LZ4"),
    Codec("DoubleDelta, ZSTD"),
    Codec("Gorilla"),
    Codec("Gorilla, LZ4"),
    Codec("Gorilla, ZSTD"),
    Codec("ALP(AUTO)"),
    Codec("ALP(AUTO), LZ4"),
    Codec("ALP(AUTO), ZSTD"),
    Codec("ALP(STD)"),
    Codec("ALP(STD), LZ4"),
    Codec("ALP(STD), ZSTD"),
    Codec("ALP(RD)"),
    Codec("ALP(RD), LZ4"),
    Codec("ALP(RD), ZSTD")
);

///////////////////////////////////////////////////////////////////////////////////////////////////
// test cases
///////////////////////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(Simple,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            makeSeq<Float64>(1, 2, 3, 5, 7, 11, 13, 17, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SmallSequences,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::ValuesIn(
                  generatePyramidOfSequences<Int8 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Int16 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Int32 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Int64 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt8 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt16>(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt32>(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt64>(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Float32>(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Float64>(42, G(SequentialGenerator(1)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(Mixed,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int8>(G(SequentialGenerator(1)), -128, 128),
            generateSeq<Int16>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int16>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int32>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int32>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int64>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int64>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt8>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt8>(G(SequentialGenerator(1)), 0, 256),
            generateSeq<UInt16>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt16>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt32>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt32>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt64>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt64>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Float32>(G(MinMaxGenerator()), 1, 5) + generateSeq<Float32>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Float64>(G(MinMaxGenerator()), 1, 5) + generateSeq<Float64>(G(SequentialGenerator(1)), 1, 1001)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SameValueInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SameValueGenerator(1000))),
            generateSeq<Int16 >(G(SameValueGenerator(1000))),
            generateSeq<Int32 >(G(SameValueGenerator(1000))),
            generateSeq<Int64 >(G(SameValueGenerator(1000))),
            generateSeq<UInt8 >(G(SameValueGenerator(1000))),
            generateSeq<UInt16>(G(SameValueGenerator(1000))),
            generateSeq<UInt32>(G(SameValueGenerator(1000))),
            generateSeq<UInt64>(G(SameValueGenerator(1000)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SameNegativeValueInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SameValueGenerator(-1000))),
            generateSeq<Int16 >(G(SameValueGenerator(-1000))),
            generateSeq<Int32 >(G(SameValueGenerator(-1000))),
            generateSeq<Int64 >(G(SameValueGenerator(-1000))),
            generateSeq<UInt8 >(G(SameValueGenerator(-1000))),
            generateSeq<UInt16>(G(SameValueGenerator(-1000))),
            generateSeq<UInt32>(G(SameValueGenerator(-1000))),
            generateSeq<UInt64>(G(SameValueGenerator(-1000)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SameValueFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("Gorilla, LZ4"),
            Codec("Gorilla, ZSTD"),
            Codec("ALP(AUTO)"),
            Codec("ALP(AUTO), LZ4"),
            Codec("ALP(AUTO), ZSTD"),
            Codec("ALP(STD)"),
            Codec("ALP(STD), LZ4"),
            Codec("ALP(STD), ZSTD"),
            Codec("ALP(RD)"),
            Codec("ALP(RD), LZ4"),
            Codec("ALP(RD), ZSTD")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SameValueGenerator(std::numbers::e_v<Float32>))),
            generateSeq<Float64>(G(SameValueGenerator(std::numbers::e_v<Float64>)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SameNegativeValueFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("Gorilla, LZ4"),
            Codec("Gorilla, ZSTD"),
            Codec("ALP(AUTO)"),
            Codec("ALP(AUTO), LZ4"),
            Codec("ALP(AUTO), ZSTD"),
            Codec("ALP(STD)"),
            Codec("ALP(STD), LZ4"),
            Codec("ALP(STD), ZSTD"),
            Codec("ALP(RD)"),
            Codec("ALP(RD), LZ4"),
            Codec("ALP(RD), ZSTD")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SameValueGenerator(-std::numbers::e_v<Float32>))),
            generateSeq<Float64>(G(SameValueGenerator(-std::numbers::e_v<Float64>)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SequentialInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SequentialGenerator(1)), 1, 128),
            generateSeq<Int16 >(G(SequentialGenerator(1))),
            generateSeq<Int32 >(G(SequentialGenerator(1))),
            generateSeq<Int64 >(G(SequentialGenerator(1))),
            generateSeq<UInt8 >(G(SequentialGenerator(1)), 1, 128),
            generateSeq<UInt16>(G(SequentialGenerator(1))),
            generateSeq<UInt32>(G(SequentialGenerator(1))),
            generateSeq<UInt64>(G(SequentialGenerator(1)))
        )
    )
);

// -1, -2, -3, ... etc for signed
// 0xFF, 0xFE, 0xFD, ... for unsigned
INSTANTIATE_TEST_SUITE_P(SequentialReverseInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SequentialGenerator(-1)), 1, 128),
            generateSeq<Int16 >(G(SequentialGenerator(-1))),
            generateSeq<Int32 >(G(SequentialGenerator(-1))),
            generateSeq<Int64 >(G(SequentialGenerator(-1))),
            generateSeq<UInt8 >(G(SequentialGenerator(-1)), 0, 256),
            generateSeq<UInt16>(G(SequentialGenerator(-1))),
            generateSeq<UInt32>(G(SequentialGenerator(-1))),
            generateSeq<UInt64>(G(SequentialGenerator(-1)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SequentialFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("Gorilla, LZ4"),
            Codec("Gorilla, ZSTD"),
            Codec("ALP(AUTO)"),
            Codec("ALP(AUTO), LZ4"),
            Codec("ALP(AUTO), ZSTD"),
            Codec("ALP(STD)"),
            Codec("ALP(STD), LZ4"),
            Codec("ALP(STD), ZSTD"),
            Codec("ALP(RD)"),
            Codec("ALP(RD), LZ4"),
            Codec("ALP(RD), ZSTD")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SequentialGenerator(std::numbers::e_v<Float32>))),
            generateSeq<Float64>(G(SequentialGenerator(std::numbers::e_v<Float64>)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SequentialReverseFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("Gorilla, LZ4"),
            Codec("Gorilla, ZSTD"),
            Codec("ALP(AUTO)"),
            Codec("ALP(AUTO), LZ4"),
            Codec("ALP(AUTO), ZSTD"),
            Codec("ALP(STD)"),
            Codec("ALP(STD), LZ4"),
            Codec("ALP(STD), ZSTD"),
            Codec("ALP(RD)"),
            Codec("ALP(RD), LZ4"),
            Codec("ALP(RD), ZSTD")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SequentialGenerator(-std::numbers::e_v<Float32>))),
            generateSeq<Float64>(G(SequentialGenerator(-std::numbers::e_v<Float64>)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(MonotonicInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(MonotonicGenerator(1, 5))),
            generateSeq<Int16>(G(MonotonicGenerator(1, 5))),
            generateSeq<Int32>(G(MonotonicGenerator(1, 5))),
            generateSeq<Int64>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt8 >(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt16>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt32>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt64>(G(MonotonicGenerator(1, 5)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(MonotonicReverseInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int16>(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int32>(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int64>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt8>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt16>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt32>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt64>(G(MonotonicGenerator(-1, 5)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(MonotonicFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("ALP(AUTO)"),
            Codec("ALP(STD)"),
            Codec("ALP(RD)")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MonotonicGenerator<Float32>(std::numbers::e_v<Float32>, 5))),
            generateSeq<Float64>(G(MonotonicGenerator<Float64>(std::numbers::e_v<Float64>, 5)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(MonotonicReverseFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("ALP(AUTO)"),
            Codec("ALP(STD)"),
            Codec("ALP(RD)")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MonotonicGenerator<Float32>(-std::numbers::e_v<Float32>, 5))),
            generateSeq<Float64>(G(MonotonicGenerator<Float64>(-std::numbers::e_v<Float64>, 5)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(RandomInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<UInt8 >(G(RandomGenerator<uint8_t>(0))),
            generateSeq<UInt16>(G(RandomGenerator<UInt16>(0))),
            generateSeq<UInt32>(G(RandomGenerator<UInt32>(0, 0, 1000'000'000))),
            generateSeq<UInt64>(G(RandomGenerator<UInt64>(0, 0, 1000'000'000)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(RandomishInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int32>(G(RandomishGenerator)),
            generateSeq<Int64>(G(RandomishGenerator)),
            generateSeq<UInt32>(G(RandomishGenerator)),
            generateSeq<UInt64>(G(RandomishGenerator)),
            generateSeq<Float32>(G(RandomishGenerator)),
            generateSeq<Float64>(G(RandomishGenerator))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(RandomishFloat,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Float32>(G(RandomishGenerator)),
            generateSeq<Float64>(G(RandomishGenerator))
        )
    )
);

// Double delta overflow case, deltas are out of bounds for target type
INSTANTIATE_TEST_SUITE_P(OverflowInt,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("DoubleDelta", 1.2),
            Codec("DoubleDelta, LZ4", 1.0)
        ),
        ::testing::Values(
            generateSeq<UInt32>(G(MinMaxGenerator())),
            generateSeq<Int32>(G(MinMaxGenerator())),
            generateSeq<UInt64>(G(MinMaxGenerator())),
            generateSeq<Int64>(G(MinMaxGenerator()))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(OverflowFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla", 1.1),
            Codec("Gorilla, LZ4", 1.0),
            Codec("Gorilla, ZSTD", 1.0),
            Codec("ALP(AUTO)", 1.1),
            Codec("ALP(AUTO), LZ4", 1.0),
            Codec("ALP(AUTO), ZSTD", 1.0),
            Codec("ALP(STD)", 1.1),
            Codec("ALP(STD), LZ4", 1.0),
            Codec("ALP(STD), ZSTD", 1.0),
            Codec("ALP(RD)", 1.1),
            Codec("ALP(RD), LZ4", 1.0),
            Codec("ALP(RD), ZSTD", 1.0)
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MinMaxGenerator())),
            generateSeq<Float64>(G(MinMaxGenerator())),
            generateSeq<Float32>(G(FFand0Generator())),
            generateSeq<Float64>(G(FFand0Generator()))
        )
    )
);

/// Size of data after ZSTD is not a multiple of 8,
/// and may break DoubleDelta.
INSTANTIATE_TEST_SUITE_P(DoubleDeltaUnalignedTranscode,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ZSTD, DoubleDelta")
        ),
        ::testing::Values(
            makeSeq<Float64>(0, 1),
            makeSeq<Float64>(1, 0)
        )
    )
);


template <typename ValueType>
auto DDCompatibilityTestSequence()
{
    // Generates sequences with double delta in given range.
    auto dd_generator = [prev_delta = static_cast<Int64>(0), prev = static_cast<Int64>(0)](auto dd) mutable
    {
        const auto curr = dd + prev + prev_delta;
        prev = curr;
        prev_delta = dd + prev_delta;
        return curr;
    };

    auto ret = generateSeq<ValueType>(G(SameValueGenerator(42)), 0, 3);

    // These values are from DoubleDelta paper (and implementation) and represent points at which DD encoded length is changed.
    // DD value less that this point is encoded in shorter binary form (bigger - longer binary).
    const Int64 dd_corner_points[] = {-63, 64, -255, 256, -2047, 2048, std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max()};
    for (const auto & p : dd_corner_points)
    {
        if (std::abs(p) > std::numeric_limits<ValueType>::max())
        {
            break;
        }

        // - 4 is to allow DD value to settle before transitioning through important point,
        // since DD depends on 2 previous values of data, + 2 is arbitrary.
        ret.append(generateSeq<ValueType>(G(dd_generator), p - 4, p + 2));
    }

    return ret;
}

#define BIN_STR(x) std::string{x, sizeof(x) - 1}

INSTANTIATE_TEST_SUITE_P(DoubleDelta,
    CodecTestCompatibility,
    ::testing::Combine(
        ::testing::Values(Codec("DoubleDelta")),
        ::testing::ValuesIn(std::initializer_list<std::tuple<CodecTestSequence, std::string>>{
            {
                DDCompatibilityTestSequence<Int8>(),
                BIN_STR("\x94\x21\x00\x00\x00\x0f\x00\x00\x00\x01\x00\x0f\x00\x00\x00\x2a\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xb1\xaa\xf4\xf6\x7d\x87\xf8\x80")
            },
            {
                DDCompatibilityTestSequence<UInt8>(),
                BIN_STR("\x94\x27\x00\x00\x00\x15\x00\x00\x00\x01\x00\x15\x00\x00\x00\x2a\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xb1\xaa\xf4\xf6\x7d\x87\xf8\x81\x8e\xd0\xca\x02\x01\x01")
            },
            {
                DDCompatibilityTestSequence<Int16>(),
                BIN_STR("\x94\x70\x00\x00\x00\x4e\x00\x00\x00\x02\x00\x27\x00\x00\x00\x2a\x00\x00\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xbc\xe3\x5d\xa3\xd3\xd9\xf6\x1f\xe2\x07\x7c\x47\x20\x67\x48\x07\x47\xff\x47\xf6\xfe\xf8\x00\x00\x70\x6b\xd0\x00\x02\x83\xd9\xfb\x9f\xdc\x1f\xfc\x20\x1e\x80\x00\x22\xc8\xf0\x00\x00\x66\x67\xa0\x00\x02\x00\x3d\x00\x00\x0f\xff\xe8\x00\x00\x7f\xee\xff\xdf\x40\x00\x0f\xf2\x78\x00\x01\x7f\x83\x9f\xf7\x9f\xfb\xc0\x00\x00\xff\xfe\x00\x00\x08\x00")
            },
            {
                DDCompatibilityTestSequence<UInt16>(),
                BIN_STR("\x94\x70\x00\x00\x00\x4e\x00\x00\x00\x02\x00\x27\x00\x00\x00\x2a\x00\x00\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xbc\xe3\x5d\xa3\xd3\xd9\xf6\x1f\xe2\x07\x7c\x47\x20\x67\x48\x07\x47\xff\x47\xf6\xfe\xf8\x00\x00\x70\x6b\xd0\x00\x02\x83\xd9\xfb\x9f\xdc\x1f\xfc\x20\x1e\x80\x00\x22\xc8\xf0\x00\x00\x66\x67\xa0\x00\x02\x00\x3d\x00\x00\x0f\xff\xe8\x00\x00\x7f\xee\xff\xdf\x40\x00\x0f\xf2\x78\x00\x01\x7f\x83\x9f\xf7\x9f\xfb\xc0\x00\x00\xff\xfe\x00\x00\x08\x00")
            },
            {
                DDCompatibilityTestSequence<Int32>(),
                BIN_STR("\x94\x74\x00\x00\x00\x9c\x00\x00\x00\x04\x00\x27\x00\x00\x00\x2a\x00\x00\x00\x00\x00\x00\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xbc\xe3\x5d\xa3\xd3\xd9\xf6\x1f\xe2\x07\x7c\x47\x20\x67\x48\x07\x47\xff\x47\xf6\xfe\xf8\x00\x00\x70\x6b\xd0\x00\x02\x83\xd9\xfb\x9f\xdc\x1f\xfc\x20\x1e\x80\x00\x22\xc8\xf0\x00\x00\x66\x67\xa0\x00\x02\x00\x3d\x00\x00\x0f\xff\xe8\x00\x00\x7f\xee\xff\xdf\x00\x00\x70\x0d\x7a\x00\x02\x80\x7b\x9f\xf7\x9f\xfb\xc0\x00\x00\xff\xfe\x00\x00\x08\x00")
            },
            {
                DDCompatibilityTestSequence<UInt32>(),
                BIN_STR("\x94\xb5\x00\x00\x00\xcc\x00\x00\x00\x04\x00\x33\x00\x00\x00\x2a\x00\x00\x00\x00\x00\x00\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xbc\xe3\x5d\xa3\xd3\xd9\xf6\x1f\xe2\x07\x7c\x47\x20\x67\x48\x07\x47\xff\x47\xf6\xfe\xf8\x00\x00\x70\x6b\xd0\x00\x02\x83\xd9\xfb\x9f\xdc\x1f\xfc\x20\x1e\x80\x00\x22\xc8\xf0\x00\x00\x66\x67\xa0\x00\x02\x00\x3d\x00\x00\x0f\xff\xe8\x00\x00\x7f\xee\xff\xdf\x00\x00\x70\x0d\x7a\x00\x02\x80\x7b\x9f\xf7\x9f\xfb\xc0\x00\x00\xff\xfe\x00\x00\x08\x00\xf3\xff\xf9\x41\xaf\xbf\xff\xd6\x0c\xfc\xff\xff\xff\xfb\xf0\x00\x00\x00\x07\xff\xff\xff\xef\xc0\x00\x00\x00\x3f\xff\xff\xff\xfb\xff\xff\xff\xfa\x69\x74\xf3\xff\xff\xff\xe7\x9f\xff\xff\xff\x7e\x00\x00\x00\x00\xff\xff\xff\xfd\xf8\x00\x00\x00\x07\xff\xff\xff\xf0")
            },
            {
                DDCompatibilityTestSequence<Int64>(),
                BIN_STR("\x94\xd4\x00\x00\x00\x98\x01\x00\x00\x08\x00\x33\x00\x00\x00\x2a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xbc\xe3\x5d\xa3\xd3\xd9\xf6\x1f\xe2\x07\x7c\x47\x20\x67\x48\x07\x47\xff\x47\xf6\xfe\xf8\x00\x00\x70\x6b\xd0\x00\x02\x83\xd9\xfb\x9f\xdc\x1f\xfc\x20\x1e\x80\x00\x22\xc8\xf0\x00\x00\x66\x67\xa0\x00\x02\x00\x3d\x00\x00\x0f\xff\xe8\x00\x00\x7f\xee\xff\xdf\x00\x00\x70\x0d\x7a\x00\x02\x80\x7b\x9f\xf7\x9f\xfb\xc0\x00\x00\xff\xfe\x00\x00\x08\x00\xfc\x00\x00\x00\x04\x00\x06\xbe\x4f\xbf\xff\xd6\x0c\xff\x00\x00\x00\x01\x00\x00\x00\x03\xf8\x00\x00\x00\x08\x00\x00\x00\x0f\xc0\x00\x00\x00\x3f\xff\xff\xff\xfb\xff\xff\xff\xfb\xe0\x00\x00\x01\xc0\x00\x00\x06\x9f\x80\x00\x00\x0a\x00\x00\x00\x34\xf3\xff\xff\xff\xe7\x9f\xff\xff\xff\x7e\x00\x00\x00\x00\xff\xff\xff\xfd\xf0\x00\x00\x00\x07\xff\xff\xff\xf0")
            },
            {
                DDCompatibilityTestSequence<UInt64>(),
                BIN_STR("\x94\xd4\x00\x00\x00\x98\x01\x00\x00\x08\x00\x33\x00\x00\x00\x2a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x6b\x65\x5f\x50\x34\xff\x4f\xaf\xbc\xe3\x5d\xa3\xd3\xd9\xf6\x1f\xe2\x07\x7c\x47\x20\x67\x48\x07\x47\xff\x47\xf6\xfe\xf8\x00\x00\x70\x6b\xd0\x00\x02\x83\xd9\xfb\x9f\xdc\x1f\xfc\x20\x1e\x80\x00\x22\xc8\xf0\x00\x00\x66\x67\xa0\x00\x02\x00\x3d\x00\x00\x0f\xff\xe8\x00\x00\x7f\xee\xff\xdf\x00\x00\x70\x0d\x7a\x00\x02\x80\x7b\x9f\xf7\x9f\xfb\xc0\x00\x00\xff\xfe\x00\x00\x08\x00\xfc\x00\x00\x00\x04\x00\x06\xbe\x4f\xbf\xff\xd6\x0c\xff\x00\x00\x00\x01\x00\x00\x00\x03\xf8\x00\x00\x00\x08\x00\x00\x00\x0f\xc0\x00\x00\x00\x3f\xff\xff\xff\xfb\xff\xff\xff\xfb\xe0\x00\x00\x01\xc0\x00\x00\x06\x9f\x80\x00\x00\x0a\x00\x00\x00\x34\xf3\xff\xff\xff\xe7\x9f\xff\xff\xff\x7e\x00\x00\x00\x00\xff\xff\xff\xfd\xf0\x00\x00\x00\x07\xff\xff\xff\xf0")
            },
        })
    )
);

template <typename ValueType>
auto DDperformanceTestSequence()
{
    const auto times = 100'000;
    return DDCompatibilityTestSequence<ValueType>() * times // average case
        + generateSeq<ValueType>(G(MinMaxGenerator()), 0, times) // worst
        + generateSeq<ValueType>(G(SameValueGenerator(42)), 0, times); // best
}

// prime numbers in ascending order with some random repetitions hit all the cases of Gorilla.
// auto PrimesWithMultiplierGenerator = [](int multiplier = 1)
// {
//     return [multiplier](auto i)
//     {
//         static const int vals[] = {
//              2, 3, 5, 7, 11, 11, 13, 17, 19, 23, 29, 29, 31, 37, 41, 43,
//             47, 47, 53, 59, 61, 61, 67, 71, 73, 79, 83, 89, 89, 97, 101, 103,
//             107, 107, 109, 113, 113, 127, 127, 127
//         };
//         static const size_t count = sizeof(vals)/sizeof(vals[0]);
//
//         return static_cast<UInt64>(vals[i % count]) * multiplier;
//     };
// };

// These 'tests' try to measure performance of encoding and decoding and hence only make sense to be run locally,
// also they require pretty big data to run against and generating this data slows down startup of unit test process.
// So un-comment only at your discretion.

// Just as if all sequences from generatePyramidOfSequences were appended to one-by-one to the first one.
//template <typename T, typename Generator>
//CodecTestSequence generatePyramidSequence(const size_t sequences_count, Generator && generator, const char* generator_name)
//{
//    CodecTestSequence sequence;
//    sequence.data_type = makeDataType<T>();
//    sequence.serialized_data.reserve(sequences_count * sequences_count * sizeof(T));
//
//    for (size_t i = 1; i < sequences_count; ++i)
//    {
//        std::string name = generator_name + std::string(" from 0 to ") + std::to_string(i);
//        sequence.append(generateSeq<T>(std::forward<decltype(generator)>(generator), name.c_str(), 0, i));
//    }
//
//    return sequence;
//};

//INSTANTIATE_TEST_SUITE_P(DoubleDelta,
//    CodecTestPerformance,
//    ::testing::Combine(
//        ::testing::Values(Codec("DoubleDelta")),
//        ::testing::Values(
//            DDperformanceTestSequence<Int8 >(),
//            DDperformanceTestSequence<UInt8 >(),
//            DDperformanceTestSequence<Int16 >(),
//            DDperformanceTestSequence<UInt16>(),
//            DDperformanceTestSequence<Int32 >(),
//            DDperformanceTestSequence<UInt32>(),
//            DDperformanceTestSequence<Int64 >(),
//            DDperformanceTestSequence<UInt64>()
//        )
//    ),
//);

//INSTANTIATE_TEST_SUITE_P(Gorilla,
//    CodecTestPerformance,
//    ::testing::Combine(
//        ::testing::Values(Codec("Gorilla")),
//        ::testing::Values(
//            generatePyramidSequence<Int8 >(42, G(PrimesWithMultiplierGenerator())) * 6'000,
//            generatePyramidSequence<UInt8 >(42, G(PrimesWithMultiplierGenerator())) * 6'000,
//            generatePyramidSequence<Int16 >(42, G(PrimesWithMultiplierGenerator())) * 6'000,
//            generatePyramidSequence<UInt16>(42, G(PrimesWithMultiplierGenerator())) * 6'000,
//            generatePyramidSequence<Int32 >(42, G(PrimesWithMultiplierGenerator())) * 6'000,
//            generatePyramidSequence<UInt32>(42, G(PrimesWithMultiplierGenerator())) * 6'000,
//            generatePyramidSequence<Int64 >(42, G(PrimesWithMultiplierGenerator())) * 6'000,
//            generatePyramidSequence<UInt64>(42, G(PrimesWithMultiplierGenerator())) * 6'000
//        )
//    ),
//);

TEST(LZ4Test, DecompressMalformedInput)
{
    /// This malformed input was initially found by lz4_decompress_fuzzer and causes failure under UBSAN.
    constexpr unsigned char data[]
        = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00,
           0x00, 0x20, 0x00, 0x00, 0x66, 0x66, 0x66, 0x66, 0xff, 0xff, 0xff, 0x17, 0xff, 0xff, 0x0f, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
           0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
           0xfe, 0x1f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    const char * const source = reinterpret_cast<const char * const>(data);
    const uint32_t source_size = std::size(data);
    constexpr uint32_t uncompressed_size = 80;

    DB::Memory<> memory;
    memory.resize(ICompressionCodec::getHeaderSize() + uncompressed_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
    unalignedStoreLittleEndian<uint8_t>(memory.data(), static_cast<uint8_t>(CompressionMethodByte::LZ4));
    unalignedStoreLittleEndian<uint32_t>(&memory[1], source_size);
    unalignedStoreLittleEndian<uint32_t>(&memory[5], uncompressed_size);

    auto codec = CompressionCodecFactory::instance().get("LZ4", {});
    ASSERT_THROW(codec->decompress(source, source_size, memory.data()), Exception);
}

TEST(DoubleDeltaTest, TranscodeRawInput)
{
    DataTypes types = {
        std::make_shared<DataTypeInt8>(),
        std::make_shared<DataTypeInt16>(),
        std::make_shared<DataTypeInt32>(),
        std::make_shared<DataTypeInt64>(),
        std::make_shared<DataTypeUInt8>(),
        std::make_shared<DataTypeUInt16>(),
        std::make_shared<DataTypeUInt32>(),
        std::make_shared<DataTypeUInt64>(),
        std::make_shared<DataTypeFloat32>(),
        std::make_shared<DataTypeFloat64>(),
    };

    for (const auto & type : types)
    {
        for (size_t buffer_size = 1; buffer_size < 40; buffer_size++)
        {
            DB::Memory<> source_memory;
            source_memory.resize(buffer_size);

            for (size_t i = 0; i < buffer_size; ++i)
                source_memory.data()[i] = static_cast<char>(i);

            DB::Memory<> memory_for_compression;
            memory_for_compression.resize(ICompressionCodec::getHeaderSize() + buffer_size);

            auto codec = makeCodec("DoubleDelta", type);

            auto compressed = codec->compress(source_memory.data(), UInt32(source_memory.size()), memory_for_compression.data());

            DB::Memory<> memory_for_decompression;
            memory_for_decompression.resize(buffer_size);
            auto decompressed = codec->decompress(memory_for_compression.data(), compressed, memory_for_decompression.data());

            ASSERT_EQ(decompressed, source_memory.size());
            for (size_t i = 0; i < decompressed; ++i)
                ASSERT_EQ(memory_for_decompression.data()[i], source_memory.data()[i]) << "with data type " << type->getName() << " with buffer size " << buffer_size << " at position " << i;
        }
    }
}

TEST(T64Test, TranscodeRawInput)
{
    DataTypes types = {
        std::make_shared<DataTypeInt8>(),
        std::make_shared<DataTypeInt16>(),
        std::make_shared<DataTypeInt32>(),
        std::make_shared<DataTypeInt64>(),
        std::make_shared<DataTypeUInt8>(),
        std::make_shared<DataTypeUInt16>(),
        std::make_shared<DataTypeUInt32>(),
        std::make_shared<DataTypeUInt64>(),
    };

    for (const auto & type : types)
    {
        for (size_t buffer_size = 1; buffer_size < 2000; buffer_size++)
        {
            DB::Memory<> source_memory;
            source_memory.resize(buffer_size);

            for (size_t i = 0; i < buffer_size; ++i)
                source_memory.data()[i] = static_cast<char>(i);

            DB::Memory<> memory_for_compression;
            auto codec = makeCodec("T64", type);

            memory_for_compression.resize(codec->getCompressedReserveSize(static_cast<UInt32>(buffer_size)));

            auto compressed = codec->compress(source_memory.data(), UInt32(source_memory.size()), memory_for_compression.data());

            DB::Memory<> memory_for_decompression;
            memory_for_decompression.resize(buffer_size);
            auto decompressed = codec->decompress(memory_for_compression.data(), compressed, memory_for_decompression.data());

            ASSERT_EQ(decompressed, source_memory.size());
            for (size_t i = 0; i < decompressed; ++i)
                ASSERT_EQ(memory_for_decompression.data()[i], source_memory.data()[i]) << "with data type " << type->getName() << " with buffer size " << buffer_size << " at position " << i;
        }
    }
}

TEST(T64Test, DecompressMalformedInputBytesToSkip)
{
    /// Reproducer for heap-buffer-overflow when `bytes_to_skip > bytes_size`
    /// in `decompressData`. Cookie 0x84 -> UInt64; bytes_to_skip = 26757 % 8 = 5,
    /// but only 1 payload byte remains after the cookie.
    constexpr unsigned char block[] = {
        0x93,                   /// T64 method byte
        0x0B, 0x00, 0x00, 0x00, /// compressed_size = 11 (9-byte header + 2-byte payload)
        0x85, 0x68, 0x00, 0x00, /// decompressed_size = 26757
        0x84, 0x2C,             /// cookie 0x84 = UInt64/Bit; bytes_to_skip=5 > bytes_size=1
    };

    const char * source = reinterpret_cast<const char *>(block);
    const UInt32 source_size = static_cast<UInt32>(std::size(block));

    DB::Memory<> dest;
    dest.resize(26757);

    auto codec = makeCodec("T64", std::make_shared<DataTypeUInt64>());
    ASSERT_THROW(codec->decompress(source, source_size, dest.data()), Exception);
}

TEST(T64Test, DecompressMalformedInputShortHeader)
{
    /// Reproducer for heap-buffer-overflow when payload has no bytes_to_skip
    /// but is shorter than the 16-byte min/max header required by T64.
    /// decompressed_size=8 → bytes_to_skip = 8 % 8 = 0; bytes_size=8 < header_size=16.
    constexpr unsigned char block[] = {
        0x93,                   /// T64 method byte
        0x11, 0x00, 0x00, 0x00, /// compressed_size = 17 (9-byte header + 8-byte payload)
        0x08, 0x00, 0x00, 0x00, /// decompressed_size = 8 (bytes_to_skip = 8 % 8 = 0)
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, /// 8-byte payload; bytes_size=8 < header_size=16
    };

    const char * source = reinterpret_cast<const char *>(block);
    const UInt32 source_size = static_cast<UInt32>(std::size(block));

    DB::Memory<> dest;
    dest.resize(8);

    auto codec = makeCodec("T64", std::make_shared<DataTypeUInt64>());
    ASSERT_THROW(codec->decompress(source, source_size, dest.data()), Exception);
}

TEST(CompressionCodecMultipleTest, DecompressMalformedInputReversedRange)
{
    /// Reproducer for process abort when `compression_methods_size + 1 > source_size`:
    /// PODArray constructed from reversed pointer range → ~2^64-byte allocation.
    /// source[0] = 0x9a = 154 codecs claimed in a 1-byte payload.
    constexpr unsigned char block[] = {
        0x91,                   /// Multiple method byte
        0x0A, 0x00, 0x00, 0x00, /// compressed_size = 10 (9-byte header + 1-byte payload)
        0x00, 0x00, 0x00, 0x00, /// decompressed_size = 0
        0x9A,                   /// claims 154 codecs in a 1-byte payload
    };

    const char * source = reinterpret_cast<const char *>(block);
    const UInt32 source_size = static_cast<UInt32>(std::size(block));

    DB::Memory<> dest;
    dest.resize(64);

    auto codec = CompressionCodecFactory::instance().get(static_cast<UInt8>(CompressionMethodByte::Multiple));
    ASSERT_THROW(codec->decompress(source, source_size, dest.data()), Exception);
}

TEST(CompressionCodecMultipleTest, DecompressMalformedInputShortBlockHeader)
{
    /// Reproducer for OOB read when the compressed payload after the methods list
    /// is shorter than COMPRESSED_BLOCK_HEADER_SIZE (9 bytes).
    /// 1 codec declared; 4 bytes of compressed data follow — too short for a block header.
    constexpr unsigned char block[] = {
        0x91,                   /// Multiple method byte
        0x0F, 0x00, 0x00, 0x00, /// compressed_size = 15 (9-byte header + 6-byte payload)
        0x00, 0x00, 0x00, 0x00, /// decompressed_size = 0
        0x01,                   /// compression_methods_size = 1
        0x82,                   /// LZ4 method byte
        0x00, 0x01, 0x02, 0x03, /// 4-byte compressed data; source_size=4 < COMPRESSED_BLOCK_HEADER_SIZE=9
    };

    const char * source = reinterpret_cast<const char *>(block);
    const UInt32 source_size = static_cast<UInt32>(std::size(block));

    DB::Memory<> dest;
    dest.resize(64);

    auto codec = CompressionCodecFactory::instance().get(static_cast<UInt8>(CompressionMethodByte::Multiple));
    ASSERT_THROW(codec->decompress(source, source_size, dest.data()), Exception);
}

#if USE_SZ3
TEST(SZ3Test, DecompressRejectsOversizedInnerLosslessSize)
{
    /// Regression for an unbounded allocation in the SZ3 lossy decompression path. The generic lossy
    /// decompressor (`ALGO_INTERP` / `ALGO_LORENZO_REG` / `ALGO_INTERP_LORENZO`) inflates an internal buffer
    /// whose size is read from the (untrusted) compressed payload. A corrupted block whose `config.num`
    /// matches the trusted output size could still declare an arbitrary inner-buffer size and force a raw
    /// `malloc` of that size before any validation. The decompressor must reject such a block before it
    /// allocates the declared size.
    auto codec = makeCodec("SZ3", std::make_shared<DataTypeFloat64>());

    /// A smooth, highly compressible sequence so SZ3 keeps the lossy (generic) algorithm rather than falling
    /// back to the bit-exact lossless path; the lossy round-trip below confirms which path was taken.
    constexpr size_t num_values = 8192;
    std::vector<Float64> values(num_values);
    for (size_t i = 0; i < num_values; ++i)
        values[i] = std::sin(static_cast<double>(i) * 0.001) * 100.0;

    const char * source = reinterpret_cast<const char *>(values.data());
    const UInt32 source_size = static_cast<UInt32>(values.size() * sizeof(Float64));

    PODArray<char> encoded(codec->getCompressedReserveSize(source_size));
    const UInt32 encoded_size = codec->compress(source, source_size, encoded.data());
    encoded.resize(encoded_size);

    /// Sanity: the unmodified block round-trips, and the result is LOSSY (differs from the input). A lossy
    /// result proves the block uses the generic interpolation/Lorenzo path - the bit-exact lossless fallback
    /// would reproduce the input exactly and would exercise a different (already-bounded) decoder.
    {
        PODArray<char> decoded(source_size);
        const UInt32 decoded_size = codec->decompress(encoded.data(), encoded_size, decoded.data());
        ASSERT_EQ(decoded_size, source_size);
        ASSERT_NE(0, memcmp(source, decoded.data(), source_size)) << "Expected a lossy (generic-path) SZ3 block";
    }

    /// The inner lossless buffer size is the 8-byte little-endian prefix of the lossless payload, which sits
    /// right after the 9-byte compressed-block header, the 1-byte SZ3 float-width byte and the 16-byte SZ3
    /// stream header.
    constexpr size_t inner_size_offset = ICompressionCodec::getHeaderSize() + 1 + 16;
    ASSERT_GT(encoded_size, inner_size_offset + sizeof(size_t));

    const size_t oversized = static_cast<size_t>(1) << 50; /// ~1 PiB, far above any legitimate inner buffer
    memcpy(encoded.data() + inner_size_offset, &oversized, sizeof(oversized));

    PODArray<char> decoded(source_size);
    bool rejected_before_allocation = false;
    try
    {
        codec->decompress(encoded.data(), encoded_size, decoded.data());
    }
    catch (const Exception & e)
    {
        rejected_before_allocation = e.message().find("exceeds the allowed capacity") != std::string::npos;
    }
    ASSERT_TRUE(rejected_before_allocation)
        << "Decompression must reject the oversized inner lossless size before allocating it";
}

namespace
{

/// An SZ3-encoded ClickHouse block produced by `CompressionCodecSZ3` is laid out as
///   [ CH codec header: getHeaderSize() bytes ][ 1-byte float width ][ SZ3 stream ]
/// where the SZ3 stream (see `SZ3/api/sz.hpp`) is
///   [ magic 4 ][ data version 4 ][ cmpDataSize 8 ][ lossless payload: cmpDataSize bytes ][ config blob ]
/// and the lossless payload (`SZ3::Lossless_zstd` framing) is
///   [ decompressed size: 8 bytes ][ zstd frame ].
/// `SZGenericCompressor` parses the decompressed lossless payload (the "inner buffer") to drive decompression.
constexpr size_t SZ3_STREAM_HEADER_SIZE = 16;

size_t sz3StreamOffset()
{
    return ICompressionCodec::getHeaderSize() + 1; /// CH block header + the 1-byte float-width prefix
}

/// Decompresses the inner buffer of a valid SZ3 block, so a test can tamper it and feed it back.
std::vector<unsigned char> sz3ExtractInnerBuffer(const char * encoded)
{
    const size_t prefix = sz3StreamOffset();
    const auto * stream = reinterpret_cast<const unsigned char *>(encoded) + prefix;
    uint64_t cmp_data_size = 0;
    memcpy(&cmp_data_size, stream + 8, sizeof(cmp_data_size)); /// skip magic (4) + version (4)
    const unsigned char * payload = stream + SZ3_STREAM_HEADER_SIZE;

    SZ3::Lossless_zstd lossless;
    unsigned char * inner = nullptr;
    size_t inner_size = 0; /// 0 capacity means "allocate, no upper bound" for this trusted, test-built block
    lossless.decompress(payload, cmp_data_size, inner, inner_size);
    std::vector<unsigned char> result(inner, inner + inner_size);
    free(inner);
    return result;
}

/// Rebuilds an SZ3 block whose inner buffer is replaced by `inner`, reusing the trailing config blob and the
/// CH/float-width prefix of `encoded`. `ICompressionCodec::decompress` reads neither a checksum nor the
/// header's compressed-size field (it takes the size from its argument), so the prefix can be reused verbatim.
std::vector<char> sz3RebuildBlockWithInner(const char * encoded, UInt32 encoded_size, const std::vector<unsigned char> & inner)
{
    const size_t prefix = sz3StreamOffset();
    const auto * stream = reinterpret_cast<const unsigned char *>(encoded) + prefix;
    const size_t stream_size = encoded_size - prefix;
    uint64_t old_cmp_data_size = 0;
    memcpy(&old_cmp_data_size, stream + 8, sizeof(old_cmp_data_size));
    const unsigned char * config_blob = stream + SZ3_STREAM_HEADER_SIZE + old_cmp_data_size;
    const size_t config_blob_size = stream_size - SZ3_STREAM_HEADER_SIZE - old_cmp_data_size;

    SZ3::Lossless_zstd lossless;
    std::vector<unsigned char> payload(ZSTD_compressBound(inner.size()) + 64 + sizeof(size_t));
    const size_t new_cmp_data_size = lossless.compress(inner.data(), inner.size(), payload.data(), payload.size());

    std::vector<char> out;
    const auto * stream_chars = reinterpret_cast<const char *>(stream);
    out.insert(out.end(), encoded, encoded + prefix); /// CH header + float width (unchanged)
    out.insert(out.end(), stream_chars, stream_chars + 8); /// magic + version (unchanged)
    const auto * size_bytes = reinterpret_cast<const char *>(&new_cmp_data_size);
    out.insert(out.end(), size_bytes, size_bytes + 8); /// updated cmpDataSize
    const auto * payload_chars = reinterpret_cast<const char *>(payload.data());
    out.insert(out.end(), payload_chars, payload_chars + new_cmp_data_size);
    const auto * config_chars = reinterpret_cast<const char *>(config_blob);
    out.insert(out.end(), config_chars, config_chars + config_blob_size);
    return out;
}

}

TEST(SZ3Test, DecompressRejectsTamperedInterpolationDimensions)
{
    /// Regression for an out-of-bounds read/write in the SZ3 interpolation decompressor. `ALGO_INTERP` stores
    /// its own dimensions array inside the (untrusted) compressed payload, separate from the trusted
    /// `config.dims`. A crafted block can keep `config.num` equal to the trusted output size while declaring
    /// larger interpolation dimensions, which would make the decompressor iterate past the end of the output
    /// buffer (and past the decoded quantization vector). The decompressor must reject the mismatch first.
    auto codec = makeCodec("SZ3('ALGO_INTERP', 'ABS', 0.001)", std::make_shared<DataTypeFloat64>());

    /// A smooth, highly compressible ramp so the forced `ALGO_INTERP` is not downgraded to the plain lossless
    /// fallback (which happens for poorly compressible data); the config check below confirms the algorithm.
    constexpr size_t num_values = 8192;
    std::vector<Float64> values(num_values);
    for (size_t i = 0; i < num_values; ++i)
        values[i] = static_cast<double>(i) * 0.5;

    const char * source = reinterpret_cast<const char *>(values.data());
    const UInt32 source_size = static_cast<UInt32>(values.size() * sizeof(Float64));

    PODArray<char> encoded(codec->getCompressedReserveSize(source_size));
    const UInt32 encoded_size = codec->compress(source, source_size, encoded.data());
    encoded.resize(encoded_size);

    /// Confirm the block actually uses the interpolation algorithm (not the lossless fallback), otherwise the
    /// inner buffer would not begin with the interpolation dimensions this test tampers with.
    {
        SZ3::Config config;
        SZ_load_config(config, encoded.data() + sz3StreamOffset(), encoded_size - sz3StreamOffset());
        ASSERT_EQ(config.cmprAlgo, SZ3::ALGO_INTERP) << "Test setup expects a forced ALGO_INTERP block";
        ASSERT_EQ(config.num, num_values);
    }

    /// The interpolation decomposition writes its dimensions array first, so it occupies the leading
    /// `N * sizeof(size_t)` bytes (N == 2: {number of vectors, inner dimension}) of the inner buffer.
    std::vector<unsigned char> inner = sz3ExtractInnerBuffer(encoded.data());
    ASSERT_GE(inner.size(), 2 * sizeof(size_t));

    /// Inflate the first stored dimension so the product of the dimensions exceeds the trusted element count.
    const size_t oversized_dimension = num_values * 2;
    const size_t inner_dimension = 1;
    memcpy(inner.data(), &oversized_dimension, sizeof(oversized_dimension));
    memcpy(inner.data() + sizeof(oversized_dimension), &inner_dimension, sizeof(inner_dimension));

    std::vector<char> tampered = sz3RebuildBlockWithInner(encoded.data(), encoded_size, inner);

    PODArray<char> decoded(source_size);
    bool rejected_dimensions = false;
    try
    {
        codec->decompress(tampered.data(), static_cast<UInt32>(tampered.size()), decoded.data());
    }
    catch (const Exception & e)
    {
        rejected_dimensions = e.message().find("stored dimensions do not match") != std::string::npos;
    }
    ASSERT_TRUE(rejected_dimensions)
        << "Decompression must reject tampered interpolation dimensions before any out-of-bounds access";
}

TEST(SZ3Test, DecompressFreesScratchBufferOnTruncatedPayload)
{
    /// Regression for a memory leak (and a check that no parse step reads out of bounds) in the SZ3 generic
    /// decompression path. After the lossless layer allocates the internal scratch buffer, several parsing
    /// steps run on the (untrusted) decompressed payload and can throw (`decomposition.load`, `encoder.load`,
    /// the quantization-index count read/check, `encoder.decode`). The scratch buffer must be freed on every
    /// such path - verified here under ASan/LSan by truncating a valid inner buffer to many lengths and
    /// feeding each back, so the parser fails at different stages without leaking or crashing.
    auto codec = makeCodec("SZ3('ALGO_INTERP', 'ABS', 0.001)", std::make_shared<DataTypeFloat64>());

    constexpr size_t num_values = 8192;
    std::vector<Float64> values(num_values);
    for (size_t i = 0; i < num_values; ++i)
        values[i] = static_cast<double>(i) * 0.5;

    const char * source = reinterpret_cast<const char *>(values.data());
    const UInt32 source_size = static_cast<UInt32>(values.size() * sizeof(Float64));

    PODArray<char> encoded(codec->getCompressedReserveSize(source_size));
    const UInt32 encoded_size = codec->compress(source, source_size, encoded.data());
    encoded.resize(encoded_size);

    const std::vector<unsigned char> inner = sz3ExtractInnerBuffer(encoded.data());
    ASSERT_GE(inner.size(), 2 * sizeof(size_t));

    /// A 4-byte payload deterministically makes the very first parse step (reading the interpolation
    /// dimensions) read past the end of the scratch buffer; it must throw rather than crash, and the buffer
    /// must be freed on that path.
    {
        const std::vector<unsigned char> tiny(inner.begin(), inner.begin() + 4);
        std::vector<char> block = sz3RebuildBlockWithInner(encoded.data(), encoded_size, tiny);
        PODArray<char> decoded(source_size);
        ASSERT_THROW(
            codec->decompress(block.data(), static_cast<UInt32>(block.size()), decoded.data()), Exception);
    }

    /// Sweep truncation lengths so the parser fails at different stages; each must throw without leaking.
    bool saw_rejection = false;
    const size_t step = std::max<size_t>(1, inner.size() / 50);
    for (size_t len = 0; len < inner.size(); len += step)
    {
        const std::vector<unsigned char> truncated(inner.begin(), inner.begin() + len);
        std::vector<char> block = sz3RebuildBlockWithInner(encoded.data(), encoded_size, truncated);
        PODArray<char> decoded(source_size);
        try
        {
            codec->decompress(block.data(), static_cast<UInt32>(block.size()), decoded.data());
        }
        catch (const Exception &)
        {
            saw_rejection = true; /// expected: a truncated payload can not be fully parsed
        }
    }
    ASSERT_TRUE(saw_rejection) << "A truncated SZ3 payload must be rejected, not silently accepted";
}
#endif

/// Expects getCompressionCodecForFile to reject the block with the given error code.
void expectRejectedBlock(ReadBuffer & in, int expected_code, bool skip_to_next_block = true)
{
    UInt32 size_compressed = 0;
    UInt32 size_decompressed = 0;
    try
    {
        getCompressionCodecForFile(in, size_compressed, size_decompressed, skip_to_next_block);
        FAIL() << "Expected exception with code " << expected_code;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
    }
}

TEST(GetCompressionCodecForFileTest, ThrowsOnCompressedSizeBelowHeader)
{
    /// size_compressed (5) is below the 9-byte block header: must throw CORRUPTED_DATA.
    constexpr unsigned char block[] = {
        0,    0,    0,    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /// 16-byte checksum (ignored)
        0x82, /// LZ4 method byte
        0x05, 0x00, 0x00, 0x00, /// size_compressed = 5
        0x01, 0x00, 0x00, 0x00, /// size_decompressed = 1
    };

    ReadBufferFromMemory in(reinterpret_cast<const char *>(block), std::size(block));
    expectRejectedBlock(in, ErrorCodes::CORRUPTED_DATA);
}

TEST(GetCompressionCodecForFileTest, ThrowsOnCorruptSizeEvenWithoutSkip)
{
    /// Pin that the size checks run regardless of the flag.
    constexpr unsigned char block[] = {
        0,    0,    0,    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /// 16-byte checksum (ignored)
        0x82, /// LZ4 method byte
        0x05, 0x00, 0x00, 0x00, /// size_compressed = 5
        0x01, 0x00, 0x00, 0x00, /// size_decompressed = 1
    };

    ReadBufferFromMemory in(reinterpret_cast<const char *>(block), std::size(block));
    expectRejectedBlock(in, ErrorCodes::CORRUPTED_DATA, /*skip_to_next_block=*/false);
}

TEST(GetCompressionCodecForFileTest, ThrowsOnCompressedSizeAboveLimit)
{
    /// size_compressed (2 GiB) is above DBMS_MAX_COMPRESSED_SIZE (1 GiB): must throw TOO_LARGE_SIZE_COMPRESSED.
    constexpr unsigned char block[] = {
        0,    0,    0,    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /// 16-byte checksum (ignored)
        0x82, /// LZ4 method byte
        0x00, 0x00, 0x00, 0x80, /// size_compressed = 2 GiB
        0x01, 0x00, 0x00, 0x00, /// size_decompressed = 1
    };

    ReadBufferFromMemory in(reinterpret_cast<const char *>(block), std::size(block));
    expectRejectedBlock(in, ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);
}

TEST(GetCompressionCodecForFileTest, ThrowsOnZeroDecompressedSize)
{
    /// Decompression rejects blocks with decompressed size 0, so identification must too.
    constexpr unsigned char block[] = {
        0,    0,    0,    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /// 16-byte checksum (ignored)
        0x82, /// LZ4 method byte
        0x0D, 0x00, 0x00, 0x00, /// size_compressed = 13 (valid)
        0x00, 0x00, 0x00, 0x00, /// size_decompressed = 0
        0x01, 0x02, 0x03, 0x04, /// payload, so unguarded code would identify the codec successfully
    };

    ReadBufferFromMemory in(reinterpret_cast<const char *>(block), std::size(block));
    expectRejectedBlock(in, ErrorCodes::CORRUPTED_DATA);
}

TEST(GetCompressionCodecForFileTest, ThrowsOnMultipleSizeBelowConsumed)
{
    /// Multiple block whose declared size_compressed (10) is below the chain bytes consumed (9B header + 1B count + 2 method bytes).
    constexpr unsigned char block[] = {
        0,    0,    0,    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /// 16-byte checksum (ignored)
        0x91, /// Multiple method byte
        0x0A, 0x00, 0x00, 0x00, /// size_compressed = 10
        0x01, 0x00, 0x00, 0x00, /// size_decompressed = 1
        0x02, /// 2 codecs
        0x82, 0x82, /// two LZ4 method bytes (valid, so codec construction succeeds)
    };

    ReadBufferFromMemory in(reinterpret_cast<const char *>(block), std::size(block));
    expectRejectedBlock(in, ErrorCodes::CORRUPTED_DATA);
}

TEST(GetCompressionCodecForFileTest, DoesNotOverreadMultipleCountByteWhenSizeEqualsHeader)
{
    constexpr unsigned char block[] = {
        0,    0,    0,    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /// 16-byte checksum (ignored)
        0x91, /// Multiple method byte
        0x09, 0x00, 0x00, 0x00, /// size_compressed = 9 (== header size, so no payload follows)
        0x01, 0x00, 0x00, 0x00, /// size_decompressed = 1
        0x01, /// count byte: belongs to the next block, must NOT be read
        0x82, /// padding, so an (incorrect) read of the count byte would find real data
    };

    ReadBufferFromMemory in(reinterpret_cast<const char *>(block), std::size(block));
    expectRejectedBlock(in, ErrorCodes::CORRUPTED_DATA);
    /// The count byte at offset 25 must not have been consumed.
    EXPECT_EQ(in.count(), 16u + ICompressionCodec::getHeaderSize());
}

auto ALPSequentialGenerator = []<typename T>(T base = T{0}, T exception = T{0}, double exception_probability = 0, int decimals = 2)
{
    std::default_random_engine random_engine(17); /// NOLINT
    std::uniform_real_distribution<> random_distribution(0.0, 1.0);

    return [=](auto i) mutable
    {
        auto random_value = random_distribution(random_engine);
        if (random_value < exception_probability)
            return exception;

        T trend_k = static_cast<T>(0.1);
        T trend = base + trend_k * static_cast<T>(i);
        T oscillation = std::sin(trend);
        T round_factor = std::pow(T{10}, static_cast<T>(decimals));

        T value = trend + oscillation;
        value = std::ceil(value * round_factor) / round_factor;

        return value;
    };
};

INSTANTIATE_TEST_SUITE_P(ALPSequentialF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(STD)", 0.3),
            Codec("ALP(RD)", 0.93),
            Codec("ALP(AUTO)", 0.3)
        ),
        ::testing::Values(
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>()), 0, 1024),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>()), 0, 2048),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>()), 0, 2560),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(-50.0)), 0, 2048),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(-5000.0)), 0, 2048)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPRDSequentialF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(RD)", 0.88),
            Codec("ALP(AUTO)", 0.88) // AUTO will fall back to RD, STD would produce ratio slightly more than 1.0
        ),
        ::testing::Values(
            generateSeq<Float64>(G(RandomGenerator<Float64>(42, std::numbers::e_v<Float64>, 2 * std::numbers::e_v<Float64>)), 0, 1024),
            generateSeq<Float64>(G(RandomGenerator<Float64>(42, std::numbers::e_v<Float64>, 2 * std::numbers::e_v<Float64>)), 0, 2048),
            generateSeq<Float64>(G(RandomGenerator<Float64>(42, std::numbers::e_v<Float64>, 2 * std::numbers::e_v<Float64>)), 0, 2816)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPSequentialF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(STD)", 0.8),
            Codec("ALP(RD)", 0.9),
            Codec("ALP(AUTO)", 0.8)
        ),
        ::testing::Values(
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>()), 0, 1024),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>()), 0, 2048),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>()), 0, 2560),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(-50.0)), 0, 2048),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(-5000.0)), 0, 2048)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPRDSequentialF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(RD)", 0.87),
            Codec("ALP(AUTO)", 0.87) // AUTO will fall back to RD, STD would produce ratio slightly more than 1.0
        ),
        ::testing::Values(
            generateSeq<Float32>(G(RandomGenerator<Float32>(42, std::numbers::e_v<Float32>, 2 * std::numbers::e_v<Float32>)), 0, 1024),
            generateSeq<Float32>(G(RandomGenerator<Float32>(42, std::numbers::e_v<Float32>, 2 * std::numbers::e_v<Float32>)), 0, 2048),
            generateSeq<Float32>(G(RandomGenerator<Float32>(42, std::numbers::e_v<Float32>, 2 * std::numbers::e_v<Float32>)), 0, 2816)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPPyramidOfSequences,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(STD)"),
            Codec("ALP(RD)")
        ),
        ::testing::ValuesIn(
              generatePyramidOfSequences<Float64>(2050, G(ALPSequentialGenerator.template operator()<Float64>()))
            + generatePyramidOfSequences<Float32>(2050, G(ALPSequentialGenerator.template operator()<Float32>()))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPLongSequencesF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(STD)", 0.3),
            Codec("ALP(RD)", 0.93)
        ),
        ::testing::Values(
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>()), 0, 65536),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>()), 0, 66000),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>()), 0, 150000)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPLongSequencesF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(STD)", 0.9),
            Codec("ALP(RD)", 0.9)
        ),
        ::testing::Values(
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>()), 0, 65536),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>()), 0, 66000),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>()), 0, 150000)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPHighPrecissionFloatsF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(STD)", 0.5),
            Codec("ALP(RD)", 0.9)
        ),
        ::testing::Values(
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, 0, 0, 4)), 0, 2048),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, 0, 0, 6)), 0, 2048)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPHighPrecissionFloatsF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("ALP(STD)", 0.999),
            Codec("ALP(RD)", 0.86)
        ),
        ::testing::Values(
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, 0, 0, 4)), 0, 2048),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, 0, 0, 6)), 0, 2048)
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPSpecialFloatsF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(STD)", 0.4)),
        ::testing::Values(
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::infinity(), 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, -std::numeric_limits<Float64>::infinity(), 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::quiet_NaN(), 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::signaling_NaN(), 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::bit_cast<Float64>(0x8000000000000000ULL), 0.1))), // negative zero
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::max(), 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::min(), 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::denorm_min(), 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, 9223372036854773760.0, 0.1))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, -9223372036854773760.0, 0.1)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPSpecialFloatsF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(STD)", 0.9)),
        ::testing::Values(
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::infinity(), 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, -std::numeric_limits<Float32>::infinity(), 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::quiet_NaN(), 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::signaling_NaN(), 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::bit_cast<Float32>(0x80000000U), 0.1))), // negative zero
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::max(), 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::min(), 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::denorm_min(), 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, 9223371487098961920.0f, 0.1))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, -9223371487098961920.0f, 0.1)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPManyExceptionsF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(STD)", 0.99)),
        ::testing::Values(
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::quiet_NaN(), 0.4))),
            generateSeq<Float64>(G(ALPSequentialGenerator.template operator()<Float64>(0, std::numeric_limits<Float64>::quiet_NaN(), 0.6)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPManyExceptionsF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(STD)", 1.01)),
        ::testing::Values(
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::quiet_NaN(), 0.4))),
            generateSeq<Float32>(G(ALPSequentialGenerator.template operator()<Float32>(0, std::numeric_limits<Float32>::quiet_NaN(), 0.6)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPExceptionsOnly,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(STD)", 1.01)),
        ::testing::Values(
            generateSeq<Float64>(G([](auto) { return std::numeric_limits<Float64>::quiet_NaN(); })),
            generateSeq<Float32>(G([](auto) { return std::numeric_limits<Float32>::quiet_NaN(); })),
            generateSeq<Float64>(G([](auto) { return std::numbers::pi_v<Float64>; })),
            generateSeq<Float32>(G([](auto) { return std::numbers::pi_v<Float32>; }))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPSameValuesF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(STD)", 0.1)),
        ::testing::Values(
            generateSeq<Float64>(G([](auto) { return 2.2; })),
            generateSeq<Float64>(G([](auto) { return -2.2; })),
            generateSeq<Float64>(G([](auto) { return 0.0; }))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPSameValuesF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(STD)", 0.1)),
        ::testing::Values(
            generateSeq<Float32>(G([](auto) { return 2.2f; })),
            generateSeq<Float32>(G([](auto) { return -2.2f; })),
            generateSeq<Float32>(G([](auto) { return 0.0f; }))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPRDSameValuesF64,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(RD)", 0.77)),
        ::testing::Values(
            generateSeq<Float64>(G([](auto) { return std::numbers::pi_v<Float64>; }))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(ALPRDSameValuesF32,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(Codec("ALP(RD)", 0.52)),
        ::testing::Values(
            generateSeq<Float32>(G([](auto) { return std::numbers::pi_v<Float32>; }))
        )
    )
);

class ALPTest : public ::testing::Test
{
protected:
    static std::vector<UInt8> constructSourceWithHeader(const std::vector<UInt8> & source, UInt32 dest_size)
    {
        UInt32 source_size = static_cast<UInt32>(source.size());

        std::vector<UInt8> data = {
            // General codec header
            static_cast<UInt8>(CompressionMethodByte::ALP), // method byte
            static_cast<UInt8>(source_size & 0xFF),         // compressed size (byte 0)
            static_cast<UInt8>((source_size >> 8) & 0xFF),  // compressed size (byte 1)
            static_cast<UInt8>((source_size >> 16) & 0xFF), // compressed size (byte 2)
            static_cast<UInt8>((source_size >> 24) & 0xFF), // compressed size (byte 3)
            static_cast<UInt8>(dest_size & 0xFF),           // decompressed size (byte 0)
            static_cast<UInt8>((dest_size >> 8) & 0xFF),    // decompressed size (byte 1)
            static_cast<UInt8>((dest_size >> 16) & 0xFF),   // decompressed size (byte 2)
            static_cast<UInt8>((dest_size >> 24) & 0xFF),   // decompressed size (byte 3)
        };
        data.append_range(source);

        return data;
    }

    template <typename T = DataTypeFloat64>
    static auto verifyDecompressExpectedException(const std::vector<UInt8> & source, const std::string & expectedMessage, UInt32 dest_size = 8192)
    {
        try
        {
            std::vector<UInt8> source_with_header = constructSourceWithHeader(source, dest_size);
            auto codec = makeCodec("ALP(STD)", std::make_shared<T>());
            std::vector<char> dest(dest_size);

            codec->decompress(reinterpret_cast<const char *>(source_with_header.data()), static_cast<UInt32>(source_with_header.size()), dest.data());

            FAIL() << "Expected Exception with message: " << expectedMessage;
        }
        catch (const Exception& e)
        {
            EXPECT_EQ(expectedMessage, e.message());
        }
    }
};

TEST_F(ALPTest, SupportedFloatTypes)
{
    DataTypes supported_types = {
        std::make_shared<DataTypeFloat32>(),
        std::make_shared<DataTypeFloat64>()
    };

    for (const auto & type : supported_types)
        ASSERT_NO_THROW(makeCodec("ALP(STD)", type)) << "ALP codec should accept " << type->getName();
}

TEST_F(ALPTest, UnsupportedFloatTypes)
{
    DataTypes unsupported_types = {
        std::make_shared<DataTypeUInt32>(),
        std::make_shared<DataTypeInt32>(),
        std::make_shared<DataTypeUInt64>(),
        std::make_shared<DataTypeInt64>(),
        std::make_shared<DataTypeBFloat16>()
    };

    for (const auto & type : unsupported_types)
        ASSERT_THROW(makeCodec("ALP(STD)", type), Exception) << "ALP codec should reject " << type->getName();
}

TEST_F(ALPTest, CompressProducesCorrectHeader)
{
    const std::vector<std::tuple<std::string, DataTypePtr, UInt8, UInt8>> test_cases = {
        {"ALP(STD)", std::make_shared<DataTypeFloat64>(), 0x01, 0x08},
        {"ALP(STD)", std::make_shared<DataTypeFloat32>(), 0x01, 0x04},
        {"ALP(RD)", std::make_shared<DataTypeFloat64>(), 0x11, 0x08},
        {"ALP(RD)", std::make_shared<DataTypeFloat32>(), 0x11, 0x04}
    };

    for (const auto & [codec_name, data_type, expected_meta_byte, expected_float_width] : test_cases)
    {
        auto codec = makeCodec(codec_name, data_type);

        Memory<> source_memory;
        source_memory.resize(data_type->getSizeOfValueInMemory());
        for (size_t i = 0; i < source_memory.size(); ++i)
            source_memory.data()[i] = char {0};

        Memory<> compressed_memory;
        UInt32 compressed_size = static_cast<UInt32>(data_type->getSizeOfValueInMemory());
        compressed_memory.resize(ICompressionCodec::getHeaderSize() + codec->getCompressedReserveSize(compressed_size));

        codec->compress(source_memory.data(), static_cast<UInt32>(source_memory.size()), compressed_memory.data());

        ASSERT_EQ(compressed_memory[ICompressionCodec::getHeaderSize()], expected_meta_byte) << "for codec " << codec_name << " and data type " << data_type->getName();
        ASSERT_EQ(compressed_memory[ICompressionCodec::getHeaderSize() + 1], expected_float_width) << "for codec " << codec_name << " and data type " << data_type->getName();
    }
}

UInt8 alpAutoFloat64MetaByte(const std::vector<Float64> & values)
{
    auto codec = makeCodec("ALP(AUTO)", std::make_shared<DataTypeFloat64>());

    const UInt32 source_size = static_cast<UInt32>(values.size() * sizeof(Float64));

    Memory<> compressed_memory;
    compressed_memory.resize(ICompressionCodec::getHeaderSize() + codec->getCompressedReserveSize(source_size));

    codec->compress(reinterpret_cast<const char *>(values.data()), source_size, compressed_memory.data());

    return static_cast<UInt8>(compressed_memory[ICompressionCodec::getHeaderSize()]);
}

TEST_F(ALPTest, AutoVariantGlobalSamplingCoversWholeStream)
{
    /// With 257-511 values the presampling windows used to cluster at the head of the stream.
    /// The head is STD-hostile and the tail decimal-friendly, so STD wins only if the tail is sampled.
    std::vector<Float64> values(300);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = i < 128 ? std::sin(static_cast<Float64>(i + 1)) * 1e6 : static_cast<Float64>(i) * 0.1;

    ASSERT_EQ(alpAutoFloat64MetaByte(values), 0x01); // STD
}

TEST_F(ALPTest, AutoVariantThresholdIsSampleLengthIndependent)
{
    /// All values are STD-hostile, but the unscaled estimate of the 8-value tail sample used to stay below the full-sample threshold and forced STD.
    std::vector<Float64> values(40);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = std::sin(static_cast<Float64>(i + 1)) * 1e6;

    ASSERT_EQ(alpAutoFloat64MetaByte(values), 0x11); // RD
}

TEST_F(ALPTest, DecompressMalformedInputWithTruncatedHeader)
{
    const std::vector<UInt8> source = {
        0x01, // meta byte (version=1, variant=STD)
        0x08  // float width
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, data has wrong header");
}

TEST_F(ALPTest, DecompressMalformedInputWithInvalidFloatWidth)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x01,       // float width (invalid, should be 4 or 8)
        0x00, 0x04  // block float count
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, unsupported float width 1");
}

TEST_F(ALPTest, DecompressMalformedInputWithInvalidBlockFloatCount)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x08  // block float count equal to 2048 (invalid, should be 1024)
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, supported block float count is 1024, got 2048");
}

TEST_F(ALPTest, DecompressMalformedInputWithTruncatedBlockHeader)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x04, // block float count
        0x02,       // exponent
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, incomplete block header (encoded)");
}

TEST_F(ALPTest, DecompressMalformedInputWithInvalidExponent)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x04, // block float count
        0x7F,       // exponent
        0x00,       // fraction
        0x00, 0x00, // exception count = 0
        0x01,       // bits = 1
        // FOR base (8 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, invalid exponent value: 127, max allowed: 18");
}

TEST_F(ALPTest, DecompressMalformedInputWithInvalidFraction)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x04, // block float count
        0x00,       // exponent
        0x7F,       // fraction
        0x00, 0x00, // exception count = 0
        0x01,       // bits = 1
        // FOR base (8 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, invalid fraction value: 127, max allowed: 18");
}

TEST_F(ALPTest, DecompressMalformedInputWithTruncatedBlockData)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x04, // block float count
        0x02,       // exponent
        0x00,       // fraction
        0x00, 0x00, // exception count = 0
        0x01,       // bits = 1
        // FOR base (8 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Bitpacked data (incomplete)
        0xFF, 0xFF, 0xFF, 0xFF // only 2 bytes instead of 128 bytes
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, incomplete block payload, available size: 4, bit-width: 1, exceptions: 0");
}

TEST_F(ALPTest, DecompressMalformedInputWithInvalidExceptionsCount)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x04, // block float count
        0x02,       // exponent
        0x00,       // fraction
        0x01, 0x00, // exception count = 1
        0x01,       // bits = 1
        // FOR base (8 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Bitpacked data (128 bytes for 1024 values with 1 bit each)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        // No exceptions data, expected 1 exception
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, incomplete block payload, available size: 128, bit-width: 1, exceptions: 1");
}

TEST_F(ALPTest, DecompressMalformedInputWithInvalidExceptionIndex)
{
    const std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x04, // block float count
        0x02,       // exponent
        0x00,       // fraction
        0x01, 0x00, // exception count = 1
        0x01,       // bits = 1
        // FOR base (8 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Bitpacked data (128 bytes for 1024 values with 1 bit each)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        // Exception indices (invalid index 1025)
        0x00, 0x04,                                     // exception index (1024 bigger than max allowed index 1023)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // exception value
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, invalid exception index, index: 1024, float count: 1024");
}

TEST_F(ALPTest, DecompressMalformedInputWithTrailingBytesAfterValidPayload)
{
    std::vector<UInt8> source = {
        0x01,       // meta byte (version=1, variant=STD)
        0x08,       // float width
        0x00, 0x04, // block float count
        0x02,       // exponent
        0x00,       // fraction
        0x00, 0x00, // exception count = 0
        0x01,       // bits = 1
        // FOR base (8 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };
    // Bitpacked data (128 bytes for 1024 values with 1 bit each)
    source.resize(source.size() + 128, 0xFF);

    // Append trailing bytes after valid payload
    const std::vector<UInt8> trailing_bytes = {0xDE, 0xAD, 0xBE, 0xEF};
    source.insert(source.end(), trailing_bytes.begin(), trailing_bytes.end());

    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, stream size mismatch");
}

TEST_F(ALPTest, DecompressMalformedInputWithInvalidReservedBitsInMetaByte)
{
    const std::vector<UInt8> source = {
        0x21,       // meta byte with invalid reserved bits
        0x08,       // float width (Float64)
        0x00, 0x04  // block float count = 1024
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP-encoded data, invalid meta byte with reserved bits set: 33");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithTruncatedRDHeader)
{
    const std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04  // block float count = 1024
        // RD header is missing entirely
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, incomplete RD header");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidLeftBitWidthZero)
{
    const std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header
        0x00,       // left_bits = 0 (invalid, must be 1–16)
        0x01        // dict_size = 1
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, invalid left bit-width: 0, allowed: 1-16");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidLeftBitWidthTooLarge)
{
    const std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header
        0x11,       // left_bits = 17 (invalid, max 16)
        0x01        // dict_size = 1
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, invalid left bit-width: 17, allowed: 1-16");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidDictionarySizeZero)
{
    const std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header
        0x01,       // left_bits = 1
        0x00        // dict_size = 0 (invalid, must be 1–8)
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, invalid dictionary size: 0");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidDictionarySizeGreaterThanAllowed)
{
    const std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header
        0x01,       // left_bits = 1
        0x09        // dict_size = 9 (invalid, max 8)
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, invalid dictionary size: 9, max allowed: 8");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidDictionaryEntryGreaterThanAllowed)
{
    const std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header
        0x01,       // left_bits = 1
        0x01,       // dict_size = 1
        0x02, 0x00  // dictionary entry = 2 (invalid, max allowed: 1)
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, invalid dictionary value: 2, limit: 1");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithTruncatedBlockData)
{
    const std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header: left_bits=1, dict_size=1, one dictionary entry
        0x01,       // left_bits = 1
        0x01,       // dict_size = 1
        0x00, 0x00, // dictionary entry
        // Block: exception count = 0, but missing bitpacked data
        0x00, 0x00, // exception count = 0
        // Only 4 bytes of right data (need 8064)
        0xFF, 0xFF, 0xFF, 0xFF
    };
    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, incomplete block payload, available size: 4, left bit-width: 1, dictionary size: 1, exceptions: 0");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidExceptionsCount)
{
    std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header: left_bits=1, dict_size=1, one dictionary entry
        0x01,       // left_bits = 1
        0x01,       // dict_size = 1
        0x00, 0x00, // dictionary entry
        // Block: exception count = 1
        0x01, 0x00  // exception count = 1
    };
    // Append 8064 zero bytes for bitpacked right data (bitpacked left is 0 bytes for dict_size=1)
    source.resize(source.size() + 8064, 0x00);

    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, incomplete block payload, available size: 8064, left bit-width: 1, dictionary size: 1, exceptions: 1");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidDictionaryIndex)
{
    std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header: left_bits=1, dict_size=3, three dictionary entries
        0x02,       // left_bits = 2
        0x03,       // dict_size = 3
        0x00, 0x00, // dict entry 0
        0x01, 0x00, // dict entry 1
        0x02, 0x00, // dict entry 2
        // Block: exception count = 0
        0x00, 0x00  // exception count = 0
    };
    // Append 256 bytes of 0xFF for bitpacked left (all indices decode to 3, invalid for dict_size=3)
    source.resize(source.size() + 256, 0xFF);
    // Append 8064 zero bytes for bitpacked right
    source.resize(source.size() + 8064, 0x00);

    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, invalid dictionary index: 3, dict size: 3");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithInvalidExceptionIndex)
{
    std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header: left_bits=1, dict_size=1, one dictionary entry
        0x01,       // left_bits = 1
        0x01,       // dict_size = 1
        0x00, 0x00, // dictionary entry
        // Block: exception count = 1
        0x01, 0x00  // exception count = 1
    };
    // Append 8064 zero bytes for bitpacked right (bitpacked left is 0 bytes for dict_size=1)
    source.resize(source.size() + 8064, 0x00);
    // Exception: index=1024 (0x0400 LE) + 8 bytes value
    const std::vector<UInt8> exception_data = {
        0x00, 0x04,                                     // exception index 1024 (invalid, >= float_count)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  // exception value
    };
    source.insert(source.end(), exception_data.begin(), exception_data.end());

    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, invalid exception index, index: 1024, float count: 1024");
}

TEST_F(ALPTest, DecompressMalformedInputRDWithTrailingBytesAfterValidPayload)
{
    std::vector<UInt8> source = {
        0x11,       // meta byte (version=1, variant=RD)
        0x08,       // float width (Float64)
        0x00, 0x04, // block float count = 1024
        // RD header: left_bits=1, dict_size=1, one dictionary entry
        0x01,       // left_bits = 1
        0x01,       // dict_size = 1
        0x00, 0x00, // dictionary entry
        // Block: exception count = 0
        0x00, 0x00  // exception count = 0
    };
    // Append 8064 zero bytes for bitpacked right (bitpacked left is 0 bytes for dict_size=1)
    source.resize(source.size() + 8064, 0x00);

    // Append trailing bytes after valid payload
    const std::vector<UInt8> trailing_bytes = {0xDE, 0xAD, 0xBE, 0xEF};
    source.insert(source.end(), trailing_bytes.begin(), trailing_bytes.end());

    verifyDecompressExpectedException(source, "Cannot decompress ALP(RD)-encoded data, stream size mismatch");
}

}
