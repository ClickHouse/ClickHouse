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
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/LimitTransform.h>
#include <Common/SipHash.h>
#include <Common/UTF8Helpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/HashTable/HashMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Formats/registerFormats.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Core/Block.h>
#include <base/StringRef.h>
#include <Common/DateLUT.h>
#include <base/bit_cast.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <memory>
#include <cmath>
#include <unistd.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/container/flat_map.hpp>
#include <Common/TerminalSize.h>
#include <bit>

#include <Common/Obfuscator/Obfuscator.h>

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
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int TYPE_MISMATCH;
}

}

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseObfuscator(int argc, char ** argv)
try
{
    using namespace DB;
    namespace po = boost::program_options;

    registerFormats();

    po::options_description description = createOptionsDescription("Options", getTerminalWidth());
    description.add_options()
        ("help", "produce help message")
        ("structure,S", po::value<std::string>(), "structure of the initial table (list of column and type names)")
        ("input-format", po::value<std::string>(), "input format of the initial table data")
        ("output-format", po::value<std::string>(), "default output format")
        ("seed", po::value<std::string>(), "seed (arbitrary string), must be random string with at least 10 bytes length; note that a seed for each column is derived from this seed and a column name: you can obfuscate data for different tables and as long as you use identical seed and identical column names, the data for corresponding non-text columns for different tables will be transformed in the same way, so the data for different tables can be JOINed after obfuscation")
        ("limit", po::value<UInt64>(), "if specified - stop after generating that number of rows; the limit can be also greater than the number of source dataset - in this case it will process the dataset in a loop more than one time, using different seeds on every iteration, generating result as large as needed")
        ("silent", po::value<bool>()->default_value(false), "don't print information messages to stderr")
        ("save", po::value<std::string>(), "save the models after training to the specified file. You can use --limit 0 to skip the generation step. The file is using binary, platform-dependent, opaque serialization format. The model parameters are saved, while the seed is not.")
        ("load", po::value<std::string>(), "load the models instead of training from the specified file. The table structure must match the saved file. The seed should be specified separately, while other model parameters are loaded.")
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

    if (options.count("save") && options.count("load"))
    {
        std::cerr << "The options --save and --load cannot be used together.\n";
        return 1;
    }

    UInt64 seed = sipHash64(options["seed"].as<std::string>());

    std::string structure;

    if (options.count("structure"))
        structure = options["structure"].as<std::string>();

    std::string input_format = options["input-format"].as<std::string>();
    std::string output_format = options["output-format"].as<std::string>();

    std::string load_from_file;
    std::string save_into_file;

    if (options.count("load"))
        load_from_file = options["load"].as<std::string>();
    else if (options.count("save"))
        save_into_file = options["save"].as<std::string>();

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

    /// Create the header block
    SharedContextHolder shared_context = Context::createShared();
    auto context = Context::createGlobal(shared_context.get());
    auto context_const = WithContext(context).getContext();
    context->makeGlobalContext();

    Block header;

    ColumnsDescription schema_columns;

    if (structure.empty())
    {
        ReadBufferIterator read_buffer_iterator = [&](ColumnsDescription &)
        {
            auto file = std::make_unique<ReadBufferFromFileDescriptor>(STDIN_FILENO);

            /// stdin must be seekable
            auto res = lseek(file->getFD(), 0, SEEK_SET);
            if (-1 == res)
                throwFromErrno("Input must be seekable file (it will be read twice).", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

            return file;
        };

        schema_columns = readSchemaFromFormat(input_format, {}, read_buffer_iterator, false, context_const);
    }
    else
    {
        schema_columns = parseColumnsListFromString(structure, context_const);
    }

    auto schema_columns_info = schema_columns.getOrdinary();

    for (auto & info : schema_columns_info)
    {
        ColumnWithTypeAndName column;
        column.name = info.name;
        column.type = info.type;
        column.column = column.type->createColumn();
        header.insert(std::move(column));
    }

    ReadBufferFromFileDescriptor file_in(STDIN_FILENO);
    WriteBufferFromFileDescriptor file_out(STDOUT_FILENO);

    if (load_from_file.empty() || structure.empty())
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

    bool rewind_needed = false;
    if (load_from_file.empty())
    {
        if (!silent)
            std::cerr << "Training models\n";

        Pipe pipe(context->getInputFormat(input_format, file_in, header, max_block_size));

        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
        {
            obfuscator.train(block.getColumns());
            source_rows += block.rows();
            if (!silent)
                std::cerr << "Processed " << source_rows << " rows\n";
        }

        obfuscator.finalize();
        rewind_needed = true;
    }
    else
    {
        if (!silent)
            std::cerr << "Loading models\n";

        ReadBufferFromFile model_file_in(load_from_file);
        CompressedReadBuffer model_in(model_file_in);

        UInt8 version = 0;
        readBinary(version, model_in);
        if (version != 0)
            throw Exception("Unknown version of the model file", ErrorCodes::UNKNOWN_FORMAT_VERSION);

        readBinary(source_rows, model_in);

        Names data_types = header.getDataTypeNames();
        size_t header_size = 0;
        readBinary(header_size, model_in);
        if (header_size != data_types.size())
            throw Exception("The saved model was created for different number of columns", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

        for (size_t i = 0; i < header_size; ++i)
        {
            String type;
            readBinary(type, model_in);
            if (type != data_types[i])
                throw Exception("The saved model was created for different types of columns", ErrorCodes::TYPE_MISMATCH);
        }

        obfuscator.deserialize(model_in);
    }

    if (!save_into_file.empty())
    {
        if (!silent)
            std::cerr << "Saving models\n";

        WriteBufferFromFile model_file_out(save_into_file);
        CompressedWriteBuffer model_out(model_file_out, CompressionCodecFactory::instance().get("ZSTD", 1));

        /// You can change version on format change, it is currently set to zero.
        UInt8 version = 0;
        writeBinary(version, model_out);

        writeBinary(source_rows, model_out);

        /// We are writing the data types for validation, because the models serialization depends on the data types.
        Names data_types = header.getDataTypeNames();
        size_t header_size = data_types.size();
        writeBinary(header_size, model_out);
        for (const auto & type : data_types)
            writeBinary(type, model_out);

        /// Write the models.
        obfuscator.serialize(model_out);

        model_out.finalize();
        model_file_out.finalize();
    }

    if (!options.count("limit"))
        limit = source_rows;

    /// Generation step
    UInt64 processed_rows = 0;
    while (processed_rows < limit)
    {
        if (!silent)
            std::cerr << "Generating data\n";

        if (rewind_needed)
            file_in.rewind();

        Pipe pipe(context->getInputFormat(input_format, file_in, header, max_block_size));

        if (processed_rows + source_rows > limit)
        {
            pipe.addSimpleTransform([&](const Block & cur_header)
            {
                return std::make_shared<LimitTransform>(cur_header, limit - processed_rows, 0);
            });
        }

        QueryPipeline in_pipeline(std::move(pipe));

        auto output = context->getOutputFormatParallelIfPossible(output_format, file_out, header);
        QueryPipeline out_pipeline(std::move(output));

        PullingPipelineExecutor in_executor(in_pipeline);
        PushingPipelineExecutor out_executor(out_pipeline);

        Block block;
        out_executor.start();
        while (in_executor.pull(block))
        {
            Columns columns = obfuscator.generate(block.getColumns());
            out_executor.push(header.cloneWithColumns(columns));
            processed_rows += block.rows();
            if (!silent)
                std::cerr << "Processed " << processed_rows << " rows\n";
        }
        out_executor.finish();

        obfuscator.updateSeed();
        rewind_needed = true;
    }

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    auto code = DB::getCurrentExceptionCode();
    return code ? code : 1;
}
