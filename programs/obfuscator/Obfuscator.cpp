#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <Common/SipHash.h>
#include <Common/UTF8Helpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/HashTable/HashMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <common/StringRef.h>
#include <common/DateLUT.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <ext/bit_cast.h>
#include <memory>
#include <cmath>
#include <unistd.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/container/flat_map.hpp>
#include <Common/TerminalSize.h>


static const char * documentation = R"(
Simple tool for table data obfuscation.

It reads input table and produces output table, that retain some properties of input, but contains different data.
It allows to publish almost real production data for usage in benchmarks.

It is designed to retain the following properties of data:
- cardinalities of values (number of distinct values) for every column and for every tuple of columns;
- conditional cardinalities: number of distinct values of one column under condition on value of another column;
- probability distributions of absolute value of integers; sign of signed integers; exponent and sign for floats;
- probability distributions of length of strings;
- probability of zero values of numbers; empty strings and arrays, NULLs;
- data compression ratio when compressed with LZ77 and entropy family of codecs;
- continuity (magnitude of difference) of time values across table; continuity of floating point values.
- date component of DateTime values;
- UTF-8 validity of string values;
- string values continue to look somewhat natural.

Most of the properties above are viable for performance testing:
- reading data, filtering, aggregation and sorting will work at almost the same speed
    as on original data due to saved cardinalities, magnitudes, compression ratios, etc.

It works in deterministic fashion: you define a seed value and transform is totally determined by input data and by seed.
Some transforms are one to one and could be reversed, so you need to have large enough seed and keep it in secret.

It use some cryptographic primitives to transform data, but from the cryptographic point of view,
    it doesn't do anything properly and you should never consider the result as secure, unless you have other reasons for it.

It may retain some data you don't want to publish.

It always leave numbers 0, 1, -1 as is. Also it leaves dates, lengths of arrays and null flags exactly as in source data.
For example, you have a column IsMobile in your table with values 0 and 1. In transformed data, it will have the same value.
So, the user will be able to count exact ratio of mobile traffic.

Another example, suppose you have some private data in your table, like user email and you don't want to publish any single email address.
If your table is large enough and contain multiple different emails and there is no email that have very high frequency than all others,
    it will perfectly anonymize all data. But if you have small amount of different values in a column, it can possibly reproduce some of them.
And you should take care and look at exact algorithm, how this tool works, and probably fine tune some of it command line parameters.

This tool works fine only with reasonable amount of data (at least 1000s of rows).
)";


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


/// Model is used to transform columns with source data to columns
///  with similar by structure and by probability distributions but anonymized data.
class IModel
{
public:
    /// Call train iteratively for each block to train a model.
    virtual void train(const IColumn & column);

    /// Call finalize one time after training before generating.
    virtual void finalize();

    /// Call generate: pass source data column to obtain a column with anonymized data as a result.
    virtual ColumnPtr generate(const IColumn & column);

    /// Deterministically change seed to some other value. This can be used to generate more values than were in source.
    virtual void updateSeed();

    virtual ~IModel() = default;
};

using ModelPtr = std::unique_ptr<IModel>;


template <typename... Ts>
UInt64 hash(Ts... xs)
{
    SipHash hash;
    (hash.update(xs), ...);
    return hash.get64();
}


static UInt64 maskBits(UInt64 x, size_t num_bits)
{
    return x & ((1ULL << num_bits) - 1);
}


/// Apply Feistel network round to least significant num_bits part of x.
static UInt64 feistelRound(UInt64 x, size_t num_bits, UInt64 seed, size_t round)
{
    size_t num_bits_left_half = num_bits / 2;
    size_t num_bits_right_half = num_bits - num_bits_left_half;

    UInt64 left_half = maskBits(x >> num_bits_right_half, num_bits_left_half);
    UInt64 right_half = maskBits(x, num_bits_right_half);

    UInt64 new_left_half = right_half;
    UInt64 new_right_half = left_half ^ maskBits(hash(right_half, seed, round), num_bits_left_half);

    return (new_left_half << num_bits_left_half) ^ new_right_half;
}


/// Apply Feistel network with num_rounds to least significant num_bits part of x.
static UInt64 feistelNetwork(UInt64 x, size_t num_bits, UInt64 seed, size_t num_rounds = 4)
{
    UInt64 bits = maskBits(x, num_bits);
    for (size_t i = 0; i < num_rounds; ++i)
        bits = feistelRound(bits, num_bits, seed, i);
    return (x & ~((1ULL << num_bits) - 1)) ^ bits;
}


/// Pseudorandom permutation within set of numbers with the same log2(x).
static UInt64 transform(UInt64 x, UInt64 seed)
{
    /// Keep 0 and 1 as is.
    if (x == 0 || x == 1)
        return x;

    /// Pseudorandom permutation of two elements.
    if (x == 2 || x == 3)
        return x ^ (seed & 1);

    size_t num_leading_zeros = __builtin_clzll(x);

    return feistelNetwork(x, 64 - num_leading_zeros - 1, seed);
}


class UnsignedIntegerModel : public IModel
{
private:
    UInt64 seed;

public:
    explicit UnsignedIntegerModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        MutableColumnPtr res = column.cloneEmpty();

        size_t size = column.size();
        res->reserve(size);

        for (size_t i = 0; i < size; ++i)
            res->insert(transform(column.getUInt(i), seed));

        return res;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Keep sign and apply pseudorandom permutation after converting to unsigned as above.
static Int64 transformSigned(Int64 x, UInt64 seed)
{
    if (x >= 0)
        return transform(x, seed);
    else
        return -transform(-x, seed);    /// It works Ok even for minimum signed number.
}


class SignedIntegerModel : public IModel
{
private:
    UInt64 seed;

public:
    explicit SignedIntegerModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        MutableColumnPtr res = column.cloneEmpty();

        size_t size = column.size();
        res->reserve(size);

        for (size_t i = 0; i < size; ++i)
            res->insert(transformSigned(column.getInt(i), seed));

        return res;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Pseudorandom permutation of mantissa.
template <typename Float>
Float transformFloatMantissa(Float x, UInt64 seed)
{
    using UInt = std::conditional_t<std::is_same_v<Float, Float32>, UInt32, UInt64>;
    constexpr size_t mantissa_num_bits = std::is_same_v<Float, Float32> ? 23 : 52;

    UInt x_uint = ext::bit_cast<UInt>(x);
    x_uint = feistelNetwork(x_uint, mantissa_num_bits, seed);
    return ext::bit_cast<Float>(x_uint);
}


/// Transform difference from previous number by applying pseudorandom permutation to mantissa part of it.
/// It allows to retain some continuity property of source data.
template <typename Float>
class FloatModel : public IModel
{
private:
    UInt64 seed;
    Float src_prev_value = 0;
    Float res_prev_value = 0;

public:
    explicit FloatModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = assert_cast<const ColumnVector<Float> &>(column).getData();
        size_t size = src_data.size();

        auto res_column = ColumnVector<Float>::create(size);
        auto & res_data = assert_cast<ColumnVector<Float> &>(*res_column).getData();

        for (size_t i = 0; i < size; ++i)
        {
            res_data[i] = res_prev_value + transformFloatMantissa(src_data[i] - src_prev_value, seed);
            src_prev_value = src_data[i];
            res_prev_value = res_data[i];
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Leave all data as is. For example, it is used for columns of type Date.
class IdentityModel : public IModel
{
public:
    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        return column.cloneResized(column.size());
    }

    void updateSeed() override
    {
    }
};


/// Pseudorandom function, but keep word characters as word characters.
static void transformFixedString(const UInt8 * src, UInt8 * dst, size_t size, UInt64 seed)
{
    {
        SipHash hash;
        hash.update(seed);
        hash.update(reinterpret_cast<const char *>(src), size);
        seed = hash.get64();
    }

    UInt8 * pos = dst;
    UInt8 * end = dst + size;

    size_t i = 0;
    while (pos < end)
    {
        SipHash hash;
        hash.update(seed);
        hash.update(i);

        if (size >= 16)
        {
            char * hash_dst = reinterpret_cast<char *>(std::min(pos, end - 16));
            hash.get128(hash_dst);
        }
        else
        {
            char value[16];
            hash.get128(value);
            memcpy(dst, value, end - dst);
        }

        pos += 16;
        ++i;
    }

    for (size_t j = 0; j < size; ++j)
    {
        if (isWordCharASCII(src[j]))
        {
            static constexpr char word_chars[] = "_01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            dst[j] = word_chars[dst[j] % (sizeof(word_chars) - 1)];
        }
    }
}


class FixedStringModel : public IModel
{
private:
    UInt64 seed;

public:
    explicit FixedStringModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnFixedString & column_fixed_string = assert_cast<const ColumnFixedString &>(column);
        const size_t string_size = column_fixed_string.getN();

        const auto & src_data = column_fixed_string.getChars();
        size_t size = column_fixed_string.size();

        auto res_column = ColumnFixedString::create(string_size);
        auto & res_data = res_column->getChars();

        res_data.resize(src_data.size());

        for (size_t i = 0; i < size; ++i)
            transformFixedString(&src_data[i * string_size], &res_data[i * string_size], string_size, seed);

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Leave date part as is and apply pseudorandom permutation to time difference with previous value within the same log2 class.
class DateTimeModel : public IModel
{
private:
    UInt64 seed;
    UInt32 src_prev_value = 0;
    UInt32 res_prev_value = 0;

    const DateLUTImpl & date_lut;

public:
    explicit DateTimeModel(UInt64 seed_) : seed(seed_), date_lut(DateLUT::instance()) {}

    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = assert_cast<const ColumnVector<UInt32> &>(column).getData();
        size_t size = src_data.size();

        auto res_column = ColumnVector<UInt32>::create(size);
        auto & res_data = assert_cast<ColumnVector<UInt32> &>(*res_column).getData();

        for (size_t i = 0; i < size; ++i)
        {
            UInt32 src_datetime = src_data[i];
            UInt32 src_date = date_lut.toDate(src_datetime);

            Int32 src_diff = src_datetime - src_prev_value;
            Int32 res_diff = transformSigned(src_diff, seed);

            UInt32 new_datetime = res_prev_value + res_diff;
            UInt32 new_time = new_datetime - date_lut.toDate(new_datetime);
            res_data[i] = src_date + new_time;

            src_prev_value = src_datetime;
            res_prev_value = res_data[i];
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


struct MarkovModelParameters
{
    size_t order;
    size_t frequency_cutoff;
    size_t num_buckets_cutoff;
    size_t frequency_add;
    double frequency_desaturate;
    size_t determinator_sliding_window_size;
};


/** Actually it's not an order-N model, but a mix of order-{0..N} models.
  *
  * We calculate code point counts for every context of 0..N previous code points.
  * Then throw off some context with low amount of statistics.
  *
  * When generating data, we try to find statistics for a context of maximum order.
  * And if not found - use context of smaller order, up to 0.
  */
class MarkovModel
{
private:
    using CodePoint = UInt32;
    using NGramHash = UInt32;

    struct Histogram
    {
        UInt64 total = 0;   /// Not including count_end.
        UInt64 count_end = 0;
        using Buckets = boost::container::flat_map<CodePoint, UInt64>;
        Buckets buckets;

        void add(CodePoint code)
        {
            ++total;
            ++buckets[code];
        }

        void addEnd()
        {
            ++count_end;
        }

        CodePoint sample(UInt64 random, double end_multiplier) const
        {
            UInt64 range = total + UInt64(count_end * end_multiplier);
            if (range == 0)
                return END;

            random %= range;

            UInt64 sum = 0;
            for (const auto & elem : buckets)
            {
                sum += elem.second;
                if (sum > random)
                    return elem.first;
            }

            return END;
        }
    };

    using Table = HashMap<NGramHash, Histogram, TrivialHash>;
    Table table;

    MarkovModelParameters params;

    std::vector<CodePoint> code_points;

    /// Special code point to form context before beginning of string.
    static constexpr CodePoint BEGIN = -1;
    /// Special code point to indicate end of string.
    static constexpr CodePoint END = -2;


    static NGramHash hashContext(const CodePoint * begin, const CodePoint * end)
    {
        return CRC32Hash()(StringRef(reinterpret_cast<const char *>(begin), (end - begin) * sizeof(CodePoint)));
    }

    /// By the way, we don't have to use actual Unicode numbers. We use just arbitrary bijective mapping.
    static CodePoint readCodePoint(const char *& pos, const char * end)
    {
        size_t length = UTF8::seqLength(*pos);

        if (pos + length > end)
            length = end - pos;
        if (length > sizeof(CodePoint))
            length = sizeof(CodePoint);

        CodePoint res = 0;
        memcpy(&res, pos, length);
        pos += length;
        return res;
    }

    static bool writeCodePoint(CodePoint code, char *& pos, const char * end)
    {
        size_t length
            = (code & 0xFF000000) ? 4
            : (code & 0xFFFF0000) ? 3
            : (code & 0xFFFFFF00) ? 2
            : 1;

        if (pos + length > end)
            return false;

        memcpy(pos, &code, length);
        pos += length;
        return true;
    }

public:
    explicit MarkovModel(MarkovModelParameters params_)
        : params(std::move(params_)), code_points(params.order, BEGIN) {}

    void consume(const char * data, size_t size)
    {
        /// First 'order' number of code points are pre-filled with BEGIN.
        code_points.resize(params.order);

        const char * pos = data;
        const char * end = data + size;

        while (true)
        {
            const bool inside = pos < end;

            CodePoint next_code_point {};

            if (inside)
                next_code_point = readCodePoint(pos, end);

            for (size_t context_size = 0; context_size < params.order; ++context_size)
            {
                NGramHash context_hash = hashContext(code_points.data() + code_points.size() - context_size, code_points.data() + code_points.size());

                if (inside)
                    table[context_hash].add(next_code_point);
                else    /// if (context_size != 0 || order == 0)     /// Don't allow to break string without context (except order-0 model).
                    table[context_hash].addEnd();
            }

            if (inside)
                code_points.push_back(next_code_point);
            else
                break;
        }
    }


    void finalize()
    {
        if (params.num_buckets_cutoff)
        {
            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();

                if (histogram.buckets.size() < params.num_buckets_cutoff)
                {
                    histogram.buckets.clear();
                    histogram.total = 0;
                }
            }
        }

        if (params.frequency_cutoff)
        {
            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();
                if (!histogram.total)
                    continue;

                if (histogram.total + histogram.count_end < params.frequency_cutoff)
                {
                    histogram.buckets.clear();
                    histogram.total = 0;
                }
                else
                {
                    Histogram::Buckets new_buckets;
                    UInt64 erased_count = 0;

                    for (const auto & bucket : histogram.buckets)
                    {
                        if (bucket.second >= params.frequency_cutoff)
                            new_buckets.emplace(bucket);
                        else
                            erased_count += bucket.second;
                    }

                    histogram.buckets.swap(new_buckets);
                    histogram.total -= erased_count;
                }
            }
        }

        if (params.frequency_add)
        {
            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();
                if (!histogram.total)
                    continue;

                for (auto & bucket : histogram.buckets)
                    bucket.second += params.frequency_add;

                histogram.count_end += params.frequency_add;
                histogram.total += params.frequency_add * histogram.buckets.size();
            }
        }

        if (params.frequency_desaturate)
        {
            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();
                if (!histogram.total)
                    continue;

                double average = double(histogram.total) / histogram.buckets.size();

                UInt64 new_total = 0;
                for (auto & bucket : histogram.buckets)
                {
                    bucket.second = bucket.second * (1.0 - params.frequency_desaturate) + average * params.frequency_desaturate;
                    new_total += bucket.second;
                }

                histogram.total = new_total;
            }
        }
    }


    size_t generate(char * data, size_t desired_size, size_t buffer_size,
        UInt64 seed, const char * determinator_data, size_t determinator_size)
    {
        code_points.resize(params.order);

        char * pos = data;
        char * end = data + buffer_size;

        while (pos < end)
        {
            Table::LookupResult it;

            size_t context_size = params.order;
            while (true)
            {
                it = table.find(hashContext(code_points.data() + code_points.size() - context_size, code_points.data() + code_points.size()));
                if (it && it->getMapped().total + it->getMapped().count_end != 0)
                    break;

                if (context_size == 0)
                    break;
                --context_size;
            }

            if (!it)
                throw Exception("Logical error in markov model", ErrorCodes::LOGICAL_ERROR);

            size_t offset_from_begin_of_string = pos - data;
            size_t determinator_sliding_window_size = params.determinator_sliding_window_size;
            if (determinator_sliding_window_size > determinator_size)
                determinator_sliding_window_size = determinator_size;

            size_t determinator_sliding_window_overflow = offset_from_begin_of_string + determinator_sliding_window_size > determinator_size
                ? offset_from_begin_of_string + determinator_sliding_window_size - determinator_size : 0;

            const char * determinator_sliding_window_begin = determinator_data + offset_from_begin_of_string - determinator_sliding_window_overflow;

            SipHash hash;
            hash.update(seed);
            hash.update(determinator_sliding_window_begin, determinator_sliding_window_size);
            hash.update(determinator_sliding_window_overflow);
            UInt64 determinator = hash.get64();

            /// If string is greater than desired_size, increase probability of end.
            double end_probability_multiplier = 0;
            Int64 num_bytes_after_desired_size = (pos - data) - desired_size;

            if (num_bytes_after_desired_size > 0)
                end_probability_multiplier = std::pow(1.25, num_bytes_after_desired_size);

            CodePoint code = it->getMapped().sample(determinator, end_probability_multiplier);

            if (code == END)
                break;

            if (num_bytes_after_desired_size > 0)
            {
                /// Heuristic: break at ASCII non-alnum code point.
                /// This allows to be close to desired_size but not break natural looking words.
                if (code < 128 && !isAlphaNumericASCII(code))
                    break;
            }

            if (!writeCodePoint(code, pos, end))
                break;

            code_points.push_back(code);
        }

        return pos - data;
    }
};


/// Generate length of strings as above.
/// To generate content of strings, use
///  order-N Markov model on Unicode code points,
///  and to generate next code point use deterministic RNG
///  determined by hash of a sliding window (default 8 bytes) of source string.
/// This is intended to generate locally-similar strings from locally-similar sources.
class StringModel : public IModel
{
private:
    UInt64 seed;
    MarkovModel markov_model;

public:
    StringModel(UInt64 seed_, MarkovModelParameters params_) : seed(seed_), markov_model(std::move(params_)) {}

    void train(const IColumn & column) override
    {
        const ColumnString & column_string = assert_cast<const ColumnString &>(column);
        size_t size = column_string.size();

        for (size_t i = 0; i < size; ++i)
        {
            StringRef string = column_string.getDataAt(i);
            markov_model.consume(string.data, string.size);
        }
    }

    void finalize() override
    {
        markov_model.finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnString & column_string = assert_cast<const ColumnString &>(column);
        size_t size = column_string.size();

        auto res_column = ColumnString::create();
        res_column->reserve(size);

        std::string new_string;
        for (size_t i = 0; i < size; ++i)
        {
            StringRef src_string = column_string.getDataAt(i);
            size_t desired_string_size = transform(src_string.size, seed);
            new_string.resize(desired_string_size * 2);

            size_t actual_size = 0;
            if (desired_string_size != 0)
                actual_size = markov_model.generate(new_string.data(), desired_string_size, new_string.size(), seed, src_string.data, src_string.size);

            res_column->insertData(new_string.data(), actual_size);
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


class ArrayModel : public IModel
{
private:
    ModelPtr nested_model;

public:
    explicit ArrayModel(ModelPtr nested_model_) : nested_model(std::move(nested_model_)) {}

    void train(const IColumn & column) override
    {
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
        const IColumn & nested_column = column_array.getData();

        nested_model->train(nested_column);
    }

    void finalize() override
    {
        nested_model->finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
        const IColumn & nested_column = column_array.getData();

        ColumnPtr new_nested_column = nested_model->generate(nested_column);

        return ColumnArray::create(IColumn::mutate(std::move(new_nested_column)), IColumn::mutate(std::move(column_array.getOffsetsPtr())));
    }

    void updateSeed() override
    {
        nested_model->updateSeed();
    }
};


class NullableModel : public IModel
{
private:
    ModelPtr nested_model;

public:
    explicit NullableModel(ModelPtr nested_model_) : nested_model(std::move(nested_model_)) {}

    void train(const IColumn & column) override
    {
        const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
        const IColumn & nested_column = column_nullable.getNestedColumn();

        nested_model->train(nested_column);
    }

    void finalize() override
    {
        nested_model->finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
        const IColumn & nested_column = column_nullable.getNestedColumn();

        ColumnPtr new_nested_column = nested_model->generate(nested_column);

        return ColumnNullable::create(IColumn::mutate(std::move(new_nested_column)), IColumn::mutate(std::move(column_nullable.getNullMapColumnPtr())));
    }

    void updateSeed() override
    {
        nested_model->updateSeed();
    }
};


class ModelFactory
{
public:
    ModelPtr get(const IDataType & data_type, UInt64 seed, MarkovModelParameters markov_model_params) const
    {
        if (isInteger(data_type))
        {
            if (isUnsignedInteger(data_type))
                return std::make_unique<UnsignedIntegerModel>(seed);
            else
                return std::make_unique<SignedIntegerModel>(seed);
        }

        if (typeid_cast<const DataTypeFloat32 *>(&data_type))
            return std::make_unique<FloatModel<Float32>>(seed);

        if (typeid_cast<const DataTypeFloat64 *>(&data_type))
            return std::make_unique<FloatModel<Float64>>(seed);

        if (typeid_cast<const DataTypeDate *>(&data_type))
            return std::make_unique<IdentityModel>();

        if (typeid_cast<const DataTypeDateTime *>(&data_type))
            return std::make_unique<DateTimeModel>(seed);

        if (typeid_cast<const DataTypeString *>(&data_type))
            return std::make_unique<StringModel>(seed, markov_model_params);

        if (typeid_cast<const DataTypeFixedString *>(&data_type))
            return std::make_unique<FixedStringModel>(seed);

        if (const auto * type = typeid_cast<const DataTypeArray *>(&data_type))
            return std::make_unique<ArrayModel>(get(*type->getNestedType(), seed, markov_model_params));

        if (const auto * type = typeid_cast<const DataTypeNullable *>(&data_type))
            return std::make_unique<NullableModel>(get(*type->getNestedType(), seed, markov_model_params));

        throw Exception("Unsupported data type", ErrorCodes::NOT_IMPLEMENTED);
    }
};


class Obfuscator
{
private:
    std::vector<ModelPtr> models;

public:
    Obfuscator(const Block & header, UInt64 seed, MarkovModelParameters markov_model_params)
    {
        ModelFactory factory;

        size_t columns = header.columns();
        models.reserve(columns);

        for (const auto & elem : header)
            models.emplace_back(factory.get(*elem.type, hash(seed, elem.name), markov_model_params));
    }

    void train(const Columns & columns)
    {
        size_t size = columns.size();
        for (size_t i = 0; i < size; ++i)
            models[i]->train(*columns[i]);
    }

    void finalize()
    {
        for (auto & model : models)
            model->finalize();
    }

    Columns generate(const Columns & columns)
    {
        size_t size = columns.size();
        Columns res(size);
        for (size_t i = 0; i < size; ++i)
            res[i] = models[i]->generate(*columns[i]);
        return res;
    }

    void updateSeed()
    {
        for (auto & model : models)
            model->updateSeed();
    }
};

}

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseObfuscator(int argc, char ** argv)
try
{
    using namespace DB;
    namespace po = boost::program_options;

    po::options_description description = createOptionsDescription("Options", getTerminalWidth());
    description.add_options()
        ("help", "produce help message")
        ("structure,S", po::value<std::string>(), "structure of the initial table (list of column and type names)")
        ("input-format", po::value<std::string>(), "input format of the initial table data")
        ("output-format", po::value<std::string>(), "default output format")
        ("seed", po::value<std::string>(), "seed (arbitrary string), must be random string with at least 10 bytes length; note that a seed for each column is derived from this seed and a column name: you can obfuscate data for different tables and as long as you use identical seed and identical column names, the data for corresponding non-text columns for different tables will be transformed in the same way, so the data for different tables can be JOINed after obfuscation")
        ("limit", po::value<UInt64>(), "if specified - stop after generating that number of rows")
        ("silent", po::value<bool>()->default_value(false), "don't print information messages to stderr")
        ("order", po::value<UInt64>()->default_value(5), "order of markov model to generate strings")
        ("frequency-cutoff", po::value<UInt64>()->default_value(5), "frequency cutoff for markov model: remove all buckets with count less than specified")
        ("num-buckets-cutoff", po::value<UInt64>()->default_value(0), "cutoff for number of different possible continuations for a context: remove all histograms with less than specified number of buckets")
        ("frequency-add", po::value<UInt64>()->default_value(0), "add a constant to every count to lower probability distribution skew")
        ("frequency-desaturate", po::value<double>()->default_value(0), "0..1 - move every frequency towards average to lower probability distribution skew")
        ("determinator-sliding-window-size", po::value<UInt64>()->default_value(8), "size of a sliding window in a source string - its hash is used as a seed for RNG in markov model")
        ;

    po::parsed_options parsed = po::command_line_parser(argc, argv).options(description).run();
    po::variables_map options;
    po::store(parsed, options);

    if (options.count("help")
        || !options.count("seed")
        || !options.count("structure")
        || !options.count("input-format")
        || !options.count("output-format"))
    {
        std::cout << documentation << "\n"
            << "\nUsage: " << argv[0] << " [options] < in > out\n"
            << "\nInput must be seekable file (it will be read twice).\n"
            << "\n" << description << "\n"
            << "\nExample:\n    " << argv[0] << " --seed \"$(head -c16 /dev/urandom | base64)\" --input-format TSV --output-format TSV --structure 'CounterID UInt32, URLDomain String, URL String, SearchPhrase String, Title String' < stats.tsv\n";
        return 0;
    }

    UInt64 seed = sipHash64(options["seed"].as<std::string>());

    std::string structure = options["structure"].as<std::string>();
    std::string input_format = options["input-format"].as<std::string>();
    std::string output_format = options["output-format"].as<std::string>();

    UInt64 limit = 0;
    if (options.count("limit"))
        limit = options["limit"].as<UInt64>();

    bool silent = options["silent"].as<bool>();

    MarkovModelParameters markov_model_params;

    markov_model_params.order = options["order"].as<UInt64>();
    markov_model_params.frequency_cutoff = options["frequency-cutoff"].as<UInt64>();
    markov_model_params.num_buckets_cutoff = options["num-buckets-cutoff"].as<UInt64>();
    markov_model_params.frequency_add = options["frequency-add"].as<UInt64>();
    markov_model_params.frequency_desaturate = options["frequency-desaturate"].as<double>();
    markov_model_params.determinator_sliding_window_size = options["determinator-sliding-window-size"].as<UInt64>();

    // Create header block
    std::vector<std::string> structure_vals;
    boost::split(structure_vals, structure, boost::algorithm::is_any_of(" ,"), boost::algorithm::token_compress_on);

    if (structure_vals.size() % 2 != 0)
        throw Exception("Odd number of elements in section structure: must be a list of name type pairs", ErrorCodes::LOGICAL_ERROR);

    Block header;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (size_t i = 0, size = structure_vals.size(); i < size; i += 2)
    {
        ColumnWithTypeAndName column;
        column.name = structure_vals[i];
        column.type = data_type_factory.get(structure_vals[i + 1]);
        column.column = column.type->createColumn();
        header.insert(std::move(column));
    }

    SharedContextHolder shared_context = Context::createShared();
    Context context = Context::createGlobal(shared_context.get());
    context.makeGlobalContext();

    ReadBufferFromFileDescriptor file_in(STDIN_FILENO);
    WriteBufferFromFileDescriptor file_out(STDOUT_FILENO);

    {
        /// stdin must be seekable
        auto res = lseek(file_in.getFD(), 0, SEEK_SET);
        if (-1 == res)
            throwFromErrno("Input must be seekable file (it will be read twice).", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    }

    Obfuscator obfuscator(header, seed, markov_model_params);

    UInt64 max_block_size = 8192;

    /// Train step
    UInt64 source_rows = 0;
    {
        if (!silent)
            std::cerr << "Training models\n";

        BlockInputStreamPtr input = context.getInputFormat(input_format, file_in, header, max_block_size);

        input->readPrefix();
        while (Block block = input->read())
        {
            obfuscator.train(block.getColumns());
            source_rows += block.rows();
            if (!silent)
                std::cerr << "Processed " << source_rows << " rows\n";
        }
        input->readSuffix();
    }

    obfuscator.finalize();

    if (!limit)
        limit = source_rows;

    /// Generation step
    UInt64 processed_rows = 0;
    while (processed_rows < limit)
    {
        if (!silent)
            std::cerr << "Generating data\n";

        file_in.seek(0, SEEK_SET);

        BlockInputStreamPtr input = context.getInputFormat(input_format, file_in, header, max_block_size);
        BlockOutputStreamPtr output = context.getOutputFormat(output_format, file_out, header);

        if (processed_rows + source_rows > limit)
            input = std::make_shared<LimitBlockInputStream>(input, limit - processed_rows, 0);

        input->readPrefix();
        output->writePrefix();
        while (Block block = input->read())
        {
            Columns columns = obfuscator.generate(block.getColumns());
            output->write(header.cloneWithColumns(columns));
            processed_rows += block.rows();
            if (!silent)
                std::cerr << "Processed " << processed_rows << " rows\n";
        }
        output->writeSuffix();
        input->readSuffix();

        obfuscator.updateSeed();
    }

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    auto code = DB::getCurrentExceptionCode();
    return code ? code : 1;
}
