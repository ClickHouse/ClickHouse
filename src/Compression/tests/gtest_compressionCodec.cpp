#include <Compression/CompressionFactory.h>

#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <base/types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>

#include <random>
#include <bitset>
#include <cmath>
#include <initializer_list>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <typeinfo>
#include <vector>

#include <cstring>

/// For the expansion of gtest macros.
#if defined(__clang__)
    #pragma clang diagnostic ignored "-Wdeprecated"
#elif defined (__GNUC__) && __GNUC__ >= 9
    #pragma GCC diagnostic ignored "-Wdeprecated-copy"
#endif

#include <gtest/gtest.h>

using namespace DB;


namespace
{

template <class T> using is_pod = std::is_trivial<std::is_standard_layout<T>>;
template <class T> inline constexpr bool is_pod_v = is_pod<T>::value;


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
    assert(bits <= MAX_BITS);

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

    assert(false && "unknown datatype");
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

        current_value = unalignedLoad<T>(data);
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
            assert(false && "Invalid element_size");
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
        assert(data_type->equals(*other.data_type));

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
        unalignedStore<T>(write_pos, v);
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
        const T v = gen(static_cast<T>(i));

        unalignedStore<T>(write_pos, v);
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
    IParser::Pos token_iterator(tokens, 0);

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

    const UInt32 encoded_max_size = codec.getCompressedReserveSize(source_data.size());
    PODArray<char> encoded(encoded_max_size);

    timer.start();

    assert(source_data.data() != nullptr); // Codec assumes that source buffer is not null.
    const UInt32 encoded_size = codec.compress(source_data.data(), source_data.size(), encoded.data());
    timer.report("encoding");

    encoded.resize(encoded_size);

    PODArray<char> decoded(source_data.size());

    timer.start();
    const UInt32 decoded_size = codec.decompress(encoded.data(), encoded.size(), decoded.data());
    timer.report("decoding");

    decoded.resize(decoded_size);

    ASSERT_TRUE(EqualByteContainers(test_sequence.data_type->getSizeOfValueInMemory(), source_data, decoded));

    const auto header_size = codec.getHeaderSize();
    const auto compression_ratio = (encoded_size - header_size) / (source_data.size() * 1.0);

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
    const UInt32 encoded_max_size = codec->getCompressedReserveSize(source_data.size());
    PODArray<char> encoded(encoded_max_size);

    const UInt32 encoded_size = codec->compress(source_data.data(), source_data.size(), encoded.data());
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
    const UInt32 decoded_size = codec->decompress(encoded_data.c_str(), encoded_data.size(), decoded.data());
    decoded.resize(decoded_size);

    ASSERT_TRUE(EqualByteContainers(expected.data_type->getSizeOfValueInMemory(), expected.serialized_data, decoded));
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
            mean += v;
        }

        mean = mean / tmp_v.size();
        double std_dev = 0.0;
        for (const auto & v : tmp_v)
        {
            const auto d = (v - mean);
            std_dev += (d * d);
        }
        std_dev = std::sqrt(std_dev / tmp_v.size());

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
        return static_cast<ValueType>(stride * i);
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
          random_engine(0),
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
        : random_engine(seed),
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

auto RandomishGenerator = [](auto i)
{
    using T = decltype(i);
    double sin_value = sin(static_cast<double>(i * i)) * i;
    if (sin_value < std::numeric_limits<T>::lowest() || sin_value > static_cast<double>(std::numeric_limits<T>::max()))
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
        sequences.push_back(generateSeq<T>(std::forward<decltype(generator)>(generator), name.c_str(), 0, i));
    }

    return sequences;
};

// helper macro to produce human-friendly sequence name from generator
#define G(generator) generator, #generator

const auto DefaultCodecsToTest = ::testing::Values(
    Codec("DoubleDelta"),
    Codec("DoubleDelta, LZ4"),
    Codec("DoubleDelta, ZSTD"),
    Codec("Gorilla"),
    Codec("Gorilla, LZ4"),
    Codec("Gorilla, ZSTD")
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
        )
    )
);

INSTANTIATE_TEST_SUITE_P(Mixed,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int8>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int16>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int16>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int32>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int32>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int64>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int64>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt8>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt8>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt16>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt16>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt32>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt32>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt64>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt64>(G(SequentialGenerator(1)), 1, 1001)
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
            Codec("Gorilla, LZ4")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SameValueGenerator(M_E))),
            generateSeq<Float64>(G(SameValueGenerator(M_E)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SameNegativeValueFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("Gorilla, LZ4")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SameValueGenerator(-1 * M_E))),
            generateSeq<Float64>(G(SameValueGenerator(-1 * M_E)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SequentialInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SequentialGenerator(1))),
            generateSeq<Int16 >(G(SequentialGenerator(1))),
            generateSeq<Int32 >(G(SequentialGenerator(1))),
            generateSeq<Int64 >(G(SequentialGenerator(1))),
            generateSeq<UInt8 >(G(SequentialGenerator(1))),
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
            generateSeq<Int8>(G(SequentialGenerator(-1))),
            generateSeq<Int16 >(G(SequentialGenerator(-1))),
            generateSeq<Int32 >(G(SequentialGenerator(-1))),
            generateSeq<Int64 >(G(SequentialGenerator(-1))),
            generateSeq<UInt8 >(G(SequentialGenerator(-1))),
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
            Codec("Gorilla, LZ4")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SequentialGenerator(M_E))),
            generateSeq<Float64>(G(SequentialGenerator(M_E)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(SequentialReverseFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla"),
            Codec("Gorilla, LZ4")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(SequentialGenerator(-1 * M_E))),
            generateSeq<Float64>(G(SequentialGenerator(-1 * M_E)))
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
            Codec("Gorilla")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MonotonicGenerator<Float32>(M_E, 5))),
            generateSeq<Float64>(G(MonotonicGenerator<Float64>(M_E, 5)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(MonotonicReverseFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MonotonicGenerator<Float32>(-1 * M_E, 5))),
            generateSeq<Float64>(G(MonotonicGenerator<Float64>(-1 * M_E, 5)))
        )
    )
);

INSTANTIATE_TEST_SUITE_P(RandomInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<UInt8 >(G(RandomGenerator<UInt8>(0))),
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
            Codec("Gorilla, LZ4", 1.0)
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MinMaxGenerator())),
            generateSeq<Float64>(G(MinMaxGenerator())),
            generateSeq<Float32>(G(FFand0Generator())),
            generateSeq<Float64>(G(FFand0Generator()))
        )
    )
);

template <typename ValueType>
auto DDCompatibilityTestSequence()
{
    // Generates sequences with double delta in given range.
    auto dd_generator = [prev_delta = static_cast<Int64>(0), prev = static_cast<Int64>(0)](auto dd) mutable //-V788
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

// prime numbers in ascending order with some random repitions hit all the cases of Gorilla.
auto PrimesWithMultiplierGenerator = [](int multiplier = 1)
{
    return [multiplier](auto i)
    {
        static const int vals[] = {
             2, 3, 5, 7, 11, 11, 13, 17, 19, 23, 29, 29, 31, 37, 41, 43,
            47, 47, 53, 59, 61, 61, 67, 71, 73, 79, 83, 89, 89, 97, 101, 103,
            107, 107, 109, 113, 113, 127, 127, 127
        };
        static const size_t count = sizeof(vals)/sizeof(vals[0]);

        using T = decltype(i);
        return static_cast<T>(vals[i % count] * static_cast<T>(multiplier));
    };
};

template <typename ValueType>
auto GCompatibilityTestSequence()
{
    // Also multiply result by some factor to test large values on types that can hold those.
    return generateSeq<ValueType>(G(PrimesWithMultiplierGenerator(intExp10(sizeof(ValueType)))), 0, 42);
}

INSTANTIATE_TEST_SUITE_P(Gorilla,
    CodecTestCompatibility,
    ::testing::Combine(
        ::testing::Values(Codec("Gorilla")),
        ::testing::ValuesIn(std::initializer_list<std::tuple<CodecTestSequence, std::string>>{
            {
                GCompatibilityTestSequence<Int8>(),
                BIN_STR("\x95\x35\x00\x00\x00\x2a\x00\x00\x00\x01\x00\x2a\x00\x00\x00\x14\xe1\xdd\x25\xe5\x7b\x29\x86\xee\x2a\x16\x5a\xc5\x0b\x23\x75\x1b\x3c\xb1\x97\x8b\x5f\xcb\x43\xd9\xc5\x48\xab\x23\xaf\x62\x93\x71\x4a\x73\x0f\xc6\x0a")
            },
            {
                GCompatibilityTestSequence<UInt8>(),
                BIN_STR("\x95\x35\x00\x00\x00\x2a\x00\x00\x00\x01\x00\x2a\x00\x00\x00\x14\xe1\xdd\x25\xe5\x7b\x29\x86\xee\x2a\x16\x5a\xc5\x0b\x23\x75\x1b\x3c\xb1\x97\x8b\x5f\xcb\x43\xd9\xc5\x48\xab\x23\xaf\x62\x93\x71\x4a\x73\x0f\xc6\x0a")
            },
            {
                GCompatibilityTestSequence<Int16>(),
                BIN_STR("\x95\x52\x00\x00\x00\x54\x00\x00\x00\x02\x00\x2a\x00\x00\x00\xc8\x00\xdc\xfe\x66\xdb\x1f\x4e\xa7\xde\xdc\xd5\xec\x6e\xf7\x37\x3a\x23\xe7\x63\xf5\x6a\x8e\x99\x37\x34\xf9\xf8\x2e\x76\x35\x2d\x51\xbb\x3b\xc3\x6d\x13\xbf\x86\x53\x9e\x25\xe4\xaf\xaf\x63\xd5\x6a\x6e\x76\x35\x3a\x27\xd3\x0f\x91\xae\x6b\x33\x57\x6e\x64\xcc\x55\x81\xe4")
            },
            {
                GCompatibilityTestSequence<UInt16>(),
                BIN_STR("\x95\x52\x00\x00\x00\x54\x00\x00\x00\x02\x00\x2a\x00\x00\x00\xc8\x00\xdc\xfe\x66\xdb\x1f\x4e\xa7\xde\xdc\xd5\xec\x6e\xf7\x37\x3a\x23\xe7\x63\xf5\x6a\x8e\x99\x37\x34\xf9\xf8\x2e\x76\x35\x2d\x51\xbb\x3b\xc3\x6d\x13\xbf\x86\x53\x9e\x25\xe4\xaf\xaf\x63\xd5\x6a\x6e\x76\x35\x3a\x27\xd3\x0f\x91\xae\x6b\x33\x57\x6e\x64\xcc\x55\x81\xe4")
            },
            {
                GCompatibilityTestSequence<Int32>(),
                BIN_STR("\x95\x65\x00\x00\x00\xa8\x00\x00\x00\x04\x00\x2a\x00\x00\x00\x20\x4e\x00\x00\xe4\x57\x63\xc0\xbb\x67\xbc\xce\x91\x97\x99\x15\x9e\xe3\x36\x3f\x89\x5f\x8e\xf2\xec\x8e\xd3\xbf\x75\x43\x58\xc4\x7e\xcf\x93\x43\x38\xc6\x91\x36\x1f\xe7\xb6\x11\x6f\x02\x73\x46\xef\xe0\xec\x50\xfb\x79\xcb\x9c\x14\xfa\x13\xea\x8d\x66\x43\x48\xa0\xde\x3a\xcf\xff\x26\xe0\x5f\x93\xde\x5e\x7f\x6e\x36\x5e\xe6\xb4\x66\x5d\xb0\x0e\xc4")
            },
            {
                GCompatibilityTestSequence<UInt32>(),
                BIN_STR("\x95\x65\x00\x00\x00\xa8\x00\x00\x00\x04\x00\x2a\x00\x00\x00\x20\x4e\x00\x00\xe4\x57\x63\xc0\xbb\x67\xbc\xce\x91\x97\x99\x15\x9e\xe3\x36\x3f\x89\x5f\x8e\xf2\xec\x8e\xd3\xbf\x75\x43\x58\xc4\x7e\xcf\x93\x43\x38\xc6\x91\x36\x1f\xe7\xb6\x11\x6f\x02\x73\x46\xef\xe0\xec\x50\xfb\x79\xcb\x9c\x14\xfa\x13\xea\x8d\x66\x43\x48\xa0\xde\x3a\xcf\xff\x26\xe0\x5f\x93\xde\x5e\x7f\x6e\x36\x5e\xe6\xb4\x66\x5d\xb0\x0e\xc4")
            },
            {
                GCompatibilityTestSequence<Int64>(),
                BIN_STR("\x95\x91\x00\x00\x00\x50\x01\x00\x00\x08\x00\x2a\x00\x00\x00\x00\xc2\xeb\x0b\x00\x00\x00\x00\xe3\x2b\xa0\xa6\x19\x85\x98\xdc\x45\x74\x74\x43\xc2\x57\x41\x4c\x6e\x42\x79\xd9\x8f\x88\xa5\x05\xf3\xf1\x94\xa3\x62\x1e\x02\xdf\x05\x10\xf1\x15\x97\x35\x2a\x50\x71\x0f\x09\x6c\x89\xf7\x65\x1d\x11\xb7\xcc\x7d\x0b\x70\xc1\x86\x88\x48\x47\x87\xb6\x32\x26\xa7\x86\x87\x88\xd3\x93\x3d\xfc\x28\x68\x85\x05\x0b\x13\xc6\x5f\xd4\x70\xe1\x5e\x76\xf1\x9f\xf3\x33\x2a\x14\x14\x5e\x40\xc1\x5c\x28\x3f\xec\x43\x03\x05\x11\x91\xe8\xeb\x8e\x0a\x0e\x27\x21\x55\xcb\x39\xbc\x6a\xff\x11\x5d\x81\xa0\xa6\x10")
            },
            {
                GCompatibilityTestSequence<UInt64>(),
                BIN_STR("\x95\x91\x00\x00\x00\x50\x01\x00\x00\x08\x00\x2a\x00\x00\x00\x00\xc2\xeb\x0b\x00\x00\x00\x00\xe3\x2b\xa0\xa6\x19\x85\x98\xdc\x45\x74\x74\x43\xc2\x57\x41\x4c\x6e\x42\x79\xd9\x8f\x88\xa5\x05\xf3\xf1\x94\xa3\x62\x1e\x02\xdf\x05\x10\xf1\x15\x97\x35\x2a\x50\x71\x0f\x09\x6c\x89\xf7\x65\x1d\x11\xb7\xcc\x7d\x0b\x70\xc1\x86\x88\x48\x47\x87\xb6\x32\x26\xa7\x86\x87\x88\xd3\x93\x3d\xfc\x28\x68\x85\x05\x0b\x13\xc6\x5f\xd4\x70\xe1\x5e\x76\xf1\x9f\xf3\x33\x2a\x14\x14\x5e\x40\xc1\x5c\x28\x3f\xec\x43\x03\x05\x11\x91\xe8\xeb\x8e\x0a\x0e\x27\x21\x55\xcb\x39\xbc\x6a\xff\x11\x5d\x81\xa0\xa6\x10")
            },
        })
    )
);

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

}
