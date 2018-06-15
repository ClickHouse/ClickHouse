#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Common/SipHash.h>
#include <Common/UTF8Helpers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <common/StringRef.h>
#include <common/DateLUT.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <ext/bit_cast.h>
#include <memory>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include <common/iostream_debug_helpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

    virtual ~IModel() {}
};

using ModelPtr = std::unique_ptr<IModel>;


template <typename... Ts>
UInt64 hash(Ts... xs)
{
    SipHash hash;
    (hash.update(xs), ...);
    return hash.get64();
}


UInt64 maskBits(UInt64 x, size_t num_bits)
{
    return x & ((1 << num_bits) - 1);
}


/// Apply Feistel network round to least significant num_bits part of x.
UInt64 feistelRound(UInt64 x, size_t num_bits, UInt64 seed, size_t round)
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
UInt64 feistelNetwork(UInt64 x, size_t num_bits, UInt64 seed, size_t num_rounds = 4)
{
    UInt64 bits = maskBits(x, num_bits);
    for (size_t i = 0; i < num_rounds; ++i)
        bits = feistelRound(bits, num_bits, seed, i);
    return (x & ~((1 << num_bits) - 1)) ^ bits;
}


/// Pseudorandom permutation within set of numbers with the same log2(x).
UInt64 transform(UInt64 x, UInt64 seed)
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
    const UInt64 seed;

public:
    UnsignedIntegerModel(UInt64 seed) : seed(seed) {}

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
};


/// Keep sign and apply pseudorandom permutation after converting to unsigned as above.
Int64 transformSigned(Int64 x, UInt64 seed)
{
    if (x >= 0)
        return transform(x, seed);
    else
        return -transform(-x, seed);    /// It works Ok even for minimum signed number.
}


class SignedIntegerModel : public IModel
{
private:
    const UInt64 seed;

public:
    SignedIntegerModel(UInt64 seed) : seed(seed) {}

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
/// It allows to retain some continuouty property of source data.
template <typename Float>
class FloatModel : public IModel
{
private:
    const UInt64 seed;
    Float src_prev_value = 0;
    Float res_prev_value = 0;

public:
    FloatModel(UInt64 seed) : seed(seed) {}

    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = static_cast<const ColumnVector<Float> &>(column).getData();
        size_t size = src_data.size();

        auto res_column = ColumnVector<Float>::create(size);
        auto & res_data = static_cast<ColumnVector<Float> &>(*res_column).getData();

        for (size_t i = 0; i < size; ++i)
        {
            res_data[i] = res_prev_value + transformFloatMantissa(src_data[i] - src_prev_value, seed);
            src_prev_value = src_data[i];
            res_prev_value = res_data[i];
        }

        return res_column;
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
};


/// Leave date part as is and apply pseudorandom permutation to time difference with previous value within the same log2 class.
class DateTimeModel : public IModel
{
private:
    const UInt64 seed;
    UInt32 src_prev_value = 0;
    UInt32 res_prev_value = 0;

    const DateLUTImpl & date_lut;

public:
    DateTimeModel(UInt64 seed) : seed(seed), date_lut(DateLUT::instance()) {}

    void train(const IColumn &) override {}
    void finalize() override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = static_cast<const ColumnVector<UInt32> &>(column).getData();
        size_t size = src_data.size();

        auto res_column = ColumnVector<UInt32>::create(size);
        auto & res_data = static_cast<ColumnVector<UInt32> &>(*res_column).getData();

        for (size_t i = 0; i < size; ++i)
        {
            UInt32 src_time = src_data[i];
            UInt32 src_date = date_lut.toDate(src_time);

            Int32 src_diff = src_time - src_prev_value;
            Int32 res_diff = transform(src_diff, seed);

            UInt32 new_time = res_prev_value + res_diff;
            res_data[i] = src_date + new_time % 86400;  /// Don't care about tz changes and daylight saving time.

            src_prev_value = src_time;
            res_prev_value = res_data[i];
        }

        return res_column;
    }
};


class MarkovModel
{
private:
    using CodePoint = UInt32;
    using NGramHash = UInt32;

    struct HistogramElement
    {
        CodePoint code;
        UInt64 count;
    };

    struct Histogram
    {
        UInt32 total = 0;
        std::vector<HistogramElement> data;

        void add(CodePoint code)
        {
            ++total;

            for (auto & elem : data)
            {
                if (elem.code == code)
                {
                    ++elem.count;
                    return;
                }
            }

            data.emplace_back(HistogramElement{.code = code, .count = 1});
        }

        UInt8 sample(UInt64 random) const
        {
            random %= total;

            UInt64 sum = 0;
            for (const auto & elem : data)
            {
                sum += elem.count;
                if (sum > random)
                    return elem.code;
            }

            __builtin_unreachable();
        }
    };

    using Table = HashMap<NGramHash, Histogram, TrivialHash>;
    Table table;

    size_t order;

    std::vector<CodePoint> code_points;


    NGramHash hashContext(const CodePoint * begin, const CodePoint * end) const
    {
        return CRC32Hash()(StringRef(reinterpret_cast<const char *>(begin), (end - begin) * sizeof(CodePoint)));
    }

    /// By the way, we don't have to use actual Unicode numbers. We use just arbitary bijective mapping.
    CodePoint readCodePoint(const char *& pos, const char * end)
    {
        size_t length = UTF8::seqLength(*pos);
        if (pos + length > end)
            length = end - pos;

        CodePoint res = 0;
        memcpy(&res, pos, length);
        pos += length;
        return res;
    }

    bool writeCodePoint(CodePoint code, char *& pos, char * end)
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
    explicit MarkovModel(size_t order) : order(order) {}

    void consume(const char * data, size_t size)
    {
        code_points.clear();

        const char * pos = data;
        const char * end = data + size;

        while (pos < end)
        {
            code_points.push_back(readCodePoint(pos, end));

            for (size_t context_size = 0; context_size < order; ++context_size)
            {
                if (code_points.size() <= context_size)
                    break;

                table[hashContext(&code_points.back() - context_size, &code_points.back())].add(code_points.back());
            }
        }
    }


    void finalize()
    {
        /// TODO: Clean low frequencies.
    }


    size_t generate(char * data, size_t size,
        UInt64 seed, const char * determinator_data, size_t determinator_size)
    {
        code_points.clear();

        char * pos = data;
        char * end = data + size;

        while (pos < end)
        {
            Table::iterator it = table.end();

            size_t context_size = std::min(order, code_points.size());
            while (true)
            {
                it = table.find(hashContext(code_points.data() + code_points.size() - context_size, code_points.data() + code_points.size()));
                if (table.end() != it)
                    break;

                if (context_size == 0)
                    break;
                --context_size;
            }

            if (table.end() == it)
                throw Exception("Logical error in markov model");

            size_t offset_from_begin_of_string = pos - data;
            constexpr size_t determinator_sliding_window_size = 8;

            size_t determinator_sliding_window_overflow = offset_from_begin_of_string + determinator_sliding_window_size > determinator_size
                ? offset_from_begin_of_string + determinator_sliding_window_size - determinator_size : 0;

            const char * determinator_sliding_window_begin = determinator_data + offset_from_begin_of_string - determinator_sliding_window_overflow;

            SipHash hash;
            hash.update(seed);
            hash.update(determinator_sliding_window_begin, determinator_sliding_window_size);
            hash.update(determinator_sliding_window_overflow);
            UInt64 determinator = hash.get64();

            CodePoint code = it->second.sample(determinator);
            code_points.push_back(code);

            if (!writeCodePoint(code, pos, end))
                break;
        }

        return pos - data;
    }
};


/// Generate length of strings as above.
/// To generate content of strings, use
///  order-N Markov model on Unicode code points,
///  and to generate next code point use deterministic RNG
///  determined by hash of 8-byte sliding window of source string.
/// This is intended to generate locally-similar strings from locally-similar sources.
class StringModel : public IModel
{
private:
    UInt64 seed;
    MarkovModel markov_model{3};

public:
    StringModel(UInt64 seed) : seed(seed) {}

    void train(const IColumn & column) override
    {
        const ColumnString & column_string = static_cast<const ColumnString &>(column);
        size_t size = column_string.size();

        for (size_t i = 0; i < size; ++i)
        {
            std::cerr << i << "\n";
            StringRef string = column_string.getDataAt(i);
            markov_model.consume(string.data, string.size);
        }
    }

    void finalize() override
    {
        /// TODO cut low frequencies
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnString & column_string = static_cast<const ColumnString &>(column);
        size_t size = column_string.size();

        auto res_column = ColumnString::create();
        res_column->reserve(size);

        std::string new_string;
        for (size_t i = 0; i < size; ++i)
        {
            StringRef src_string = column_string.getDataAt(i);
            size_t desired_string_size = transform(src_string.size, seed);
            new_string.resize(desired_string_size);

            size_t actual_size = markov_model.generate(new_string.data(), desired_string_size, seed, src_string.data, src_string.size);

            res_column->insertData(new_string.data(), actual_size);
        }

        return res_column;
    }
};


class ModelFactory
{
public:
    ModelPtr get(const IDataType & data_type, UInt64 seed) const
    {
        if (data_type.isInteger())
        {
            if (data_type.isUnsignedInteger())
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
            return std::make_unique<StringModel>(seed);
        throw Exception("Unsupported data type");
    }
};


class Anonymizer
{
private:
    std::vector<ModelPtr> models;

public:
    Anonymizer(const Block & header, UInt64 seed)
    {
        ModelFactory factory;

        size_t columns = header.columns();
        models.reserve(columns);

        for (size_t i = 0; i < columns; ++i)
            models.emplace_back(factory.get(*header.getByPosition(i).type, hash(seed, i)));
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
};

}


int main(int argc, char ** argv)
try
{
    using namespace DB;
    namespace po = boost::program_options;

    po::options_description description("Main options");
    description.add_options()
        ("help", "produce help message")
        ("structure,S", po::value<std::string>(), "structure of the initial table (list of column and type names)")
        ("input-format", po::value<std::string>(), "input format of the initial table data")
        ("output-format", po::value<std::string>(), "default output format")
        ("seed", po::value<std::string>(), "seed (arbitary string), must be random string with at least 10 bytes length")
        ;

    po::parsed_options parsed = po::command_line_parser(argc, argv).options(description).run();
    po::variables_map options;
    po::store(parsed, options);

    if (options.count("help"))
    {
        /// TODO
        return 0;
    }

    UInt64 seed = sipHash64(options["seed"].as<std::string>());

    std::string structure = options["structure"].as<std::string>();
    std::string input_format = options["input-format"].as<std::string>();
    std::string output_format = options["output-format"].as<std::string>();

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

    Context context = Context::createGlobal();

    /// stdin must be seekable
    ReadBufferFromFileDescriptor file_in(STDIN_FILENO);
    WriteBufferFromFileDescriptor file_out(STDOUT_FILENO);

    Anonymizer anonymizer(header, seed);

    size_t max_block_size = 8192;

    /// Train step
    {
        BlockInputStreamPtr input = context.getInputFormat(input_format, file_in, header, max_block_size);

        input->readPrefix();
        while (Block block = input->read())
            anonymizer.train(block.getColumns());
        input->readSuffix();
    }

    anonymizer.finalize();

    /// Generation step
    {
        file_in.seek(0);

        BlockInputStreamPtr input = context.getInputFormat(input_format, file_in, header, max_block_size);
        BlockOutputStreamPtr output = context.getOutputFormat(output_format, file_out, header);

        input->readPrefix();
        output->writePrefix();
        while (Block block = input->read())
        {
            Columns columns = anonymizer.generate(block.getColumns());
            output->write(header.cloneWithColumns(columns));
        }
        output->writeSuffix();
        input->readSuffix();
    }

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    auto code = DB::getCurrentExceptionCode();
    return code ? code : 1;
}
