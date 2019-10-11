#include <Compression/CompressionFactory.h>

#include <Common/PODArray.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>

#include <boost/format.hpp>

#include <bitset>
#include <cmath>
#include <initializer_list>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <typeinfo>
#include <vector>

#include <string.h>

/// For the expansion of gtest macros.
#if defined(__clang__)
    #pragma clang diagnostic ignored "-Wdeprecated"
#elif defined (__GNUC__) && __GNUC__ >= 9
    #pragma GCC diagnostic ignored "-Wdeprecated-copy"
#endif

#include <gtest/gtest.h>

using namespace DB;

namespace std
{
template <typename T>
std::ostream & operator<<(std::ostream & ostr, const std::optional<T> & opt)
{
    if (!opt)
    {
        return ostr << "<empty optional>";
    }

    return ostr << *opt;
}

template <typename T>
std::vector<T> operator+(std::vector<T> && left, std::vector<T> && right)
{
    std::vector<T> result(std::move(left));
    std::move(std::begin(right), std::end(right), std::back_inserter(result));

    return result;
}

}

namespace
{

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

    assert(false && "unsupported size");
    return nullptr;
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

            if (++mismatching_items <= MAX_MISMATCHING_ITEMS)
            {
                result << "mismatching " << sizeof(T) << "-byte item #" << i
                   << "\nexpected: " << bin(left_value) << " (0x" << std::hex << left_value << ")"
                   << "\ngot     : " << bin(right_value) << " (0x" << std::hex << right_value << ")"
                   << std::endl;
                if (mismatching_items == MAX_MISMATCHING_ITEMS)
                {
                    result << "..." << std::endl;
                }
            }
        }
    }
    if (mismatching_items > 0)
    {
        result << "\ntotal mismatching items:" << mismatching_items << " of " << size;
    }

    return result;
}

struct Codec
{
    std::string codec_statement;
    std::optional<double> expected_compression_ratio;

    explicit Codec(std::string codec_statement_, std::optional<double> expected_compression_ratio_ = std::nullopt)
        : codec_statement(std::move(codec_statement_)),
          expected_compression_ratio(expected_compression_ratio_)
    {}

    Codec()
        : Codec(std::string())
    {}
};


struct CodecTestSequence
{
    std::string name;
    std::vector<char> serialized_data;
    DataTypePtr data_type;

    CodecTestSequence()
        : name(),
          serialized_data(),
          data_type()
    {}

    CodecTestSequence(std::string name_, std::vector<char> serialized_data_, DataTypePtr data_type_)
        : name(name_),
          serialized_data(serialized_data_),
          data_type(data_type_)
    {}

    CodecTestSequence(const CodecTestSequence &) = default;
    CodecTestSequence & operator=(const CodecTestSequence &) = default;
    CodecTestSequence(CodecTestSequence &&) = default;
    CodecTestSequence & operator=(CodecTestSequence &&) = default;
};

CodecTestSequence operator+(CodecTestSequence && left, CodecTestSequence && right)
{
    assert(left.data_type->equals(*right.data_type));

    std::vector<char> data(std::move(left.serialized_data));
    data.insert(data.end(), right.serialized_data.begin(), right.serialized_data.end());

    return CodecTestSequence{
        left.name + " + " + right.name,
        std::move(data),
        std::move(left.data_type)
    };
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
    return ostr << "Codec{"
                << "name: " << codec.codec_statement
                << ", expected_compression_ratio: " << codec.expected_compression_ratio
                << "}";
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
            (boost::format("%1% values of %2%") % std::size(vals) % type_name<T>()).str(),
            std::move(data),
            makeDataType<T>()
    };
}

template <typename T, typename Generator>
CodecTestSequence generateSeq(Generator gen, const char* gen_name, size_t Begin = 0, size_t End = 10000)
{
    assert (End >= Begin);

    std::vector<char> data(sizeof(T) * (End - Begin));
    char * write_pos = data.data();

    for (size_t i = Begin; i < End; ++i)
    {
        const T v = gen(static_cast<T>(i));
        unalignedStore<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestSequence{
            (boost::format("%1% values of %2% from %3%") % (End - Begin) % type_name<T>() % gen_name).str(),
            std::move(data),
            makeDataType<T>()
    };
}


class CodecTest : public ::testing::TestWithParam<std::tuple<Codec, CodecTestSequence>>
{
public:
    enum MakeCodecParam
    {
        CODEC_WITH_DATA_TYPE,
        CODEC_WITHOUT_DATA_TYPE,
    };

    CompressionCodecPtr makeCodec(MakeCodecParam with_data_type) const
    {
        const auto & codec_string = std::get<0>(GetParam()).codec_statement;
        const auto & data_type = with_data_type == CODEC_WITH_DATA_TYPE ? std::get<1>(GetParam()).data_type : nullptr;

        const std::string codec_statement = "(" + codec_string + ")";
        Tokens tokens(codec_statement.begin().base(), codec_statement.end().base());
        IParser::Pos token_iterator(tokens);

        Expected expected;
        ASTPtr codec_ast;
        ParserCodec parser;

        parser.parse(token_iterator, codec_ast, expected);

        return CompressionCodecFactory::instance().get(codec_ast, data_type);
    }

    void testTranscoding(ICompressionCodec & codec)
    {
        const auto & test_sequence = std::get<1>(GetParam());
        const auto & source_data = test_sequence.serialized_data;

        const UInt32 encoded_max_size = codec.getCompressedReserveSize(source_data.size());
        PODArray<char> encoded(encoded_max_size);

        const UInt32 encoded_size = codec.compress(source_data.data(), source_data.size(), encoded.data());
        encoded.resize(encoded_size);

        PODArray<char> decoded(source_data.size());
        const UInt32 decoded_size = codec.decompress(encoded.data(), encoded.size(), decoded.data());
        decoded.resize(decoded_size);

        switch (test_sequence.data_type->getSizeOfValueInMemory())
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
                FAIL() << "Invalid test sequence data type: " << test_sequence.data_type->getName();
        }
        const auto header_size = codec.getHeaderSize();
        const auto compression_ratio = (encoded_size - header_size) / (source_data.size() * 1.0);

        const auto & codec_spec = std::get<0>(GetParam());
        if (codec_spec.expected_compression_ratio)
        {
            ASSERT_LE(compression_ratio, *codec_spec.expected_compression_ratio)
                    << "\n\tdecoded size: " << source_data.size()
                    << "\n\tencoded size: " << encoded_size
                    << "(no header: " << encoded_size - header_size << ")";
        }
    }
};

TEST_P(CodecTest, TranscodingWithDataType)
{
    const auto codec = makeCodec(CODEC_WITH_DATA_TYPE);
    testTranscoding(*codec);
}

TEST_P(CodecTest, TranscodingWithoutDataType)
{
    const auto codec = makeCodec(CODEC_WITHOUT_DATA_TYPE);
    testTranscoding(*codec);
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Here we use generators to produce test payload for codecs.
// Generator is a callable that can produce infinite number of values,
// output value MUST be of the same type input value.
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
        typename std::conditional_t<std::is_integral_v<T>, std::uniform_int_distribution<T>, void>>;


template <typename T = Int32>
struct MonotonicGenerator
{
    MonotonicGenerator(T stride_ = 1, T max_step = 10)
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
    RandomGenerator(T seed = 0, T value_min = std::numeric_limits<T>::min(), T value_max = std::numeric_limits<T>::max())
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
    return static_cast<decltype(i)>(sin(static_cast<double>(i * i)) * i);
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
        if (step++ % 2 == 0)
        {
            memset(&result, 0, sizeof(result));
        }
        else
        {
            memset(&result, 0xFF, sizeof(result));
        }

        return result;
    };
};


// Makes many sequences with generator, first sequence length is 1, second is 2... up to `sequences_count`.
template <typename T, typename Generator>
std::vector<CodecTestSequence> generatePyramidOfSequences(const size_t sequences_count, Generator && generator, const char* generator_name)
{
    std::vector<CodecTestSequence> sequences;
    sequences.reserve(sequences_count);
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

INSTANTIATE_TEST_CASE_P(Simple,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            makeSeq<Float64>(1, 2, 3, 5, 7, 11, 13, 17, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)
        )
    ),
);

INSTANTIATE_TEST_CASE_P(SmallSequences,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::ValuesIn(
                  generatePyramidOfSequences<Int8  >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Int16 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Int32 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<Int64 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt8 >(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt16>(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt32>(42, G(SequentialGenerator(1)))
                + generatePyramidOfSequences<UInt64>(42, G(SequentialGenerator(1)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(Mixed,
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
    ),
);

INSTANTIATE_TEST_CASE_P(SameValueInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8  >(G(SameValueGenerator(1000))),
            generateSeq<Int16 >(G(SameValueGenerator(1000))),
            generateSeq<Int32 >(G(SameValueGenerator(1000))),
            generateSeq<Int64 >(G(SameValueGenerator(1000))),
            generateSeq<UInt8 >(G(SameValueGenerator(1000))),
            generateSeq<UInt16>(G(SameValueGenerator(1000))),
            generateSeq<UInt32>(G(SameValueGenerator(1000))),
            generateSeq<UInt64>(G(SameValueGenerator(1000)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(SameNegativeValueInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8  >(G(SameValueGenerator(-1000))),
            generateSeq<Int16 >(G(SameValueGenerator(-1000))),
            generateSeq<Int32 >(G(SameValueGenerator(-1000))),
            generateSeq<Int64 >(G(SameValueGenerator(-1000))),
            generateSeq<UInt8 >(G(SameValueGenerator(-1000))),
            generateSeq<UInt16>(G(SameValueGenerator(-1000))),
            generateSeq<UInt32>(G(SameValueGenerator(-1000))),
            generateSeq<UInt64>(G(SameValueGenerator(-1000)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(SameValueFloat,
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
    ),
);

INSTANTIATE_TEST_CASE_P(SameNegativeValueFloat,
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
    ),
);

INSTANTIATE_TEST_CASE_P(SequentialInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8  >(G(SequentialGenerator(1))),
            generateSeq<Int16 >(G(SequentialGenerator(1))),
            generateSeq<Int32 >(G(SequentialGenerator(1))),
            generateSeq<Int64 >(G(SequentialGenerator(1))),
            generateSeq<UInt8 >(G(SequentialGenerator(1))),
            generateSeq<UInt16>(G(SequentialGenerator(1))),
            generateSeq<UInt32>(G(SequentialGenerator(1))),
            generateSeq<UInt64>(G(SequentialGenerator(1)))
        )
    ),
);

// -1, -2, -3, ... etc for signed
// 0xFF, 0xFE, 0xFD, ... for unsigned
INSTANTIATE_TEST_CASE_P(SequentialReverseInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8  >(G(SequentialGenerator(-1))),
            generateSeq<Int16 >(G(SequentialGenerator(-1))),
            generateSeq<Int32 >(G(SequentialGenerator(-1))),
            generateSeq<Int64 >(G(SequentialGenerator(-1))),
            generateSeq<UInt8 >(G(SequentialGenerator(-1))),
            generateSeq<UInt16>(G(SequentialGenerator(-1))),
            generateSeq<UInt32>(G(SequentialGenerator(-1))),
            generateSeq<UInt64>(G(SequentialGenerator(-1)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(SequentialFloat,
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
    ),
);

INSTANTIATE_TEST_CASE_P(SequentialReverseFloat,
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
    ),
);

INSTANTIATE_TEST_CASE_P(MonotonicInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8  >(G(MonotonicGenerator(1, 5))),
            generateSeq<Int16 >(G(MonotonicGenerator(1, 5))),
            generateSeq<Int32 >(G(MonotonicGenerator(1, 5))),
            generateSeq<Int64 >(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt8 >(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt16>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt32>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt64>(G(MonotonicGenerator(1, 5)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(MonotonicReverseInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Int8  >(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int16 >(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int32 >(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int64 >(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt8 >(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt16>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt32>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt64>(G(MonotonicGenerator(-1, 5)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(MonotonicFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MonotonicGenerator<Float32>(M_E, 5))),
            generateSeq<Float64>(G(MonotonicGenerator<Float64>(M_E, 5)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(MonotonicReverseFloat,
    CodecTest,
    ::testing::Combine(
        ::testing::Values(
            Codec("Gorilla")
        ),
        ::testing::Values(
            generateSeq<Float32>(G(MonotonicGenerator<Float32>(-1 * M_E, 5))),
            generateSeq<Float64>(G(MonotonicGenerator<Float64>(-1 * M_E, 5)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(RandomInt,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<UInt8 >(G(RandomGenerator<UInt8>(0))),
            generateSeq<UInt16>(G(RandomGenerator<UInt16>(0))),
            generateSeq<UInt32>(G(RandomGenerator<UInt32>(0, 0, 1000'000'000))),
            generateSeq<UInt64>(G(RandomGenerator<UInt64>(0, 0, 1000'000'000)))
        )
    ),
);

INSTANTIATE_TEST_CASE_P(RandomishInt,
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
    ),
);

INSTANTIATE_TEST_CASE_P(RandomishFloat,
    CodecTest,
    ::testing::Combine(
        DefaultCodecsToTest,
        ::testing::Values(
            generateSeq<Float32>(G(RandomishGenerator)),
            generateSeq<Float64>(G(RandomishGenerator))
        )
    ),
);

// Double delta overflow case, deltas are out of bounds for target type
INSTANTIATE_TEST_CASE_P(OverflowInt,
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
    ),
);

INSTANTIATE_TEST_CASE_P(OverflowFloat,
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
    ),
);

}
