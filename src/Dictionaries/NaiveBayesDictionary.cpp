#include <Dictionaries/NaiveBayesDictionary.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <IO/ReadHelpers.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>

#include <cmath>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
}

namespace
{

String trim(std::string_view s)
{
    size_t begin = 0;
    size_t end = s.size();
    while (begin < end && isAsciiWhitespace(s[begin]))
        ++begin;
    while (end > begin && isAsciiWhitespace(s[end - 1]))
        --end;
    return String(s.substr(begin, end - begin));
}

/// Parses an explicit priors specification of the form "0=0.6,1=0.4" into a map from class to probability.
/// Whitespace around tokens is ignored, and any malformed input raises a bad arguments error.
std::map<UInt32, double> parseExplicitPriors(const String & priors_str)
{
    std::map<UInt32, double> priors;
    double total = 0.0;

    size_t pos = 0;
    while (pos < priors_str.size())
    {
        const size_t comma = priors_str.find(',', pos);
        const size_t entry_end = (comma == String::npos) ? priors_str.size() : comma;
        const std::string_view entry(priors_str.data() + pos, entry_end - pos);
        pos = (comma == String::npos) ? priors_str.size() : comma + 1;

        const size_t eq = entry.find('=');
        if (eq == std::string_view::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid priors entry '{}': expected 'class=probability'", String(entry));

        const String class_str = trim(entry.substr(0, eq));
        const String prob_str = trim(entry.substr(eq + 1));

        UInt32 class_id = 0;
        double prob = 0.0;
        try
        {
            class_id = parse<UInt32>(class_str);
            prob = parse<Float64>(prob_str);
        }
        catch (const Exception &)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Invalid priors entry '{}': could not parse class id or probability", String(entry));
        }

        if (prob <= 0.0 || prob > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prior probability for class {} must be in (0, 1], got {}", class_id, prob);

        if (!priors.emplace(class_id, prob).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate prior for class {}", class_id);

        total += prob;
    }

    if (priors.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Explicit priors specification is empty");

    if (std::fabs(total - 1.0) > 1e-6)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sum of prior probabilities must equal 1.0, got {}", total);

    return priors;
}

}


NaiveBayesDictionary::NaiveBayesDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    Configuration configuration_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , configuration(std::move(configuration_))
    , log(getLogger("NaiveBayesDictionary"))
{
    loadData();
}


void NaiveBayesDictionary::loadData()
{
    using Trainer = std::variant<
        NaiveBayesTrainer<BytePolicy>,
        NaiveBayesTrainer<CodePointPolicy>,
        NaiveBayesTrainer<TokenPolicy>>;

    Trainer trainer = [&]() -> Trainer
    {
        if (configuration.mode == "byte")
            return Trainer{std::in_place_type<NaiveBayesTrainer<BytePolicy>>, configuration.n, configuration.alpha};
        if (configuration.mode == "codepoint")
            return Trainer{std::in_place_type<NaiveBayesTrainer<CodePointPolicy>>, configuration.n, configuration.alpha};
        if (configuration.mode == "token")
            return Trainer{std::in_place_type<NaiveBayesTrainer<TokenPolicy>>, configuration.n, configuration.alpha};
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid mode '{}' for NaiveBayes dictionary. Must be 'byte', 'codepoint', or 'token'",
            configuration.mode);
    }();

    /// The block places the key columns first and the attribute columns after them. The key is the n-gram
    /// string, the first attribute is the class id, and the second attribute is the count.
    const size_t key_size = dict_struct.getKeysSize();

    MutableColumnPtr ngram_acc;
    MutableColumnPtr class_id_acc;
    MutableColumnPtr count_acc;

    BlockIO io = source_ptr->loadAll();

    io.executeWithCallbacks([&]()
    {
        DictionaryPipelineExecutor executor(io.pipeline, false);
        io.pipeline.setConcurrencyControl(false);

        Block block;
        while (executor.pull(block))
        {
            const size_t rows = block.rows();
            const auto & ngram_col = block.safeGetByPosition(0).column;
            const auto & class_id_col = block.safeGetByPosition(key_size).column;
            const auto & count_col = block.safeGetByPosition(key_size + 1).column;

            for (size_t i = 0; i < rows; ++i)
            {
                const std::string_view ngram_sv = ngram_col->getDataAt(i);
                const auto class_id = static_cast<UInt32>(class_id_col->getUInt(i));
                const UInt64 count = count_col->getUInt(i);
                std::visit([&](auto & t) { t.addNgram(class_id, ngram_sv, count); }, trainer);
            }

            if (configuration.store_source)
            {
                if (!ngram_acc)
                {
                    ngram_acc = ngram_col->cloneEmpty();
                    class_id_acc = class_id_col->cloneEmpty();
                    count_acc = count_col->cloneEmpty();
                }
                ngram_acc->insertRangeFrom(*ngram_col, 0, rows);
                class_id_acc->insertRangeFrom(*class_id_col, 0, rows);
                count_acc->insertRangeFrom(*count_col, 0, rows);
            }
        }
    });

    ProbabilityMap explicit_priors;
    for (const auto & [class_id, prob] : configuration.explicit_priors)
        explicit_priors[class_id] = prob;

    /// Finalizing the trainer computes the priors, compiles the flat model, and yields the immutable model.
    classifier.emplace(std::visit(
        [&](auto & t) -> Classifier { return Classifier{t.finalize(configuration.priors_mode, explicit_priors)}; },
        trainer));

    if (configuration.store_source && ngram_acc)
    {
        source_ngram_column = std::move(ngram_acc);
        source_class_id_column = std::move(class_id_acc);
        source_count_column = std::move(count_acc);
    }

    element_count = std::visit([](const auto & model) { return model.getElementCount(); }, *classifier);
    bytes_allocated = std::visit([](const auto & model) { return model.getAllocatedBytes(); }, *classifier);

    LOG_INFO(log, "Loaded NaiveBayes dictionary with {} n-grams, {} bytes allocated", element_count, bytes_allocated);
}


ColumnPtr NaiveBayesDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & /* attribute_type */,
    const Columns & key_columns,
    const DataTypes & /* key_types */,
    DefaultOrFilter /* default_or_filter */) const
{
    /// Only the prediction attribute (the first one, class_id) is computable. The remaining attributes
    /// describe the training source and have no meaning as a per-input prediction.
    if (attribute_name != dict_struct.attributes.front().name)
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "NaiveBayes dictionary only supports querying attribute '{}' (the predicted class), got '{}'",
            dict_struct.attributes.front().name,
            attribute_name);

    const auto * string_col = typeid_cast<const ColumnString *>(key_columns.front().get());
    if (!string_col)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary key must be a String column");

    const size_t rows = string_col->size();
    auto result = ColumnUInt32::create(rows);
    auto & result_data = result->getData();

    visitModel([&](const auto & model)
    {
        NaiveBayesScratch scratch;
        for (size_t i = 0; i < rows; ++i)
            result_data[i] = model.classify(string_col->getDataAt(i), scratch);
    });

    query_count.fetch_add(rows, std::memory_order_relaxed);
    found_count.fetch_add(rows, std::memory_order_relaxed);

    return result;
}


ColumnUInt8::Ptr NaiveBayesDictionary::hasKeys(
    const Columns & key_columns,
    const DataTypes & /* key_types */) const
{
    /// Any text input can be classified, so every key is considered present.
    const size_t rows = key_columns.front()->size();
    auto result = ColumnUInt8::create(rows);
    result->getData().assign(rows, static_cast<UInt8>(1));

    query_count.fetch_add(rows, std::memory_order_relaxed);
    found_count.fetch_add(rows, std::memory_order_relaxed);

    return result;
}


Pipe NaiveBayesDictionary::read(const Names & column_names, size_t /* max_block_size */, size_t /* num_streams */) const
{
    if (!configuration.store_source)
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Set `store_source` to true in the NAIVE_BAYES layout to support reading the training data back from the dictionary");

    const auto & key_attribute = (*dict_struct.key)[0];

    ColumnsWithTypeAndName result_columns;
    result_columns.reserve(column_names.size());
    for (const auto & column_name : column_names)
    {
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = column_name;

        if (column_name == key_attribute.name)
        {
            column_with_type.column = source_ngram_column ? source_ngram_column : key_attribute.type->createColumn();
            column_with_type.type = key_attribute.type;
        }
        else
        {
            const auto & attribute = dict_struct.getAttribute(column_name);
            if (column_name == dict_struct.attributes.front().name)
                column_with_type.column = source_class_id_column ? source_class_id_column : attribute.type->createColumn();
            else
                column_with_type.column = source_count_column ? source_count_column : attribute.type->createColumn();
            column_with_type.type = attribute.type;
        }

        result_columns.emplace_back(std::move(column_with_type));
    }

    auto source = std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block(result_columns)));
    return Pipe(std::move(source));
}


void registerDictionaryNaiveBayes(DictionaryFactory & factory);
void registerDictionaryNaiveBayes(DictionaryFactory & factory)
{
    auto create_layout = [](
        const std::string & /* full_name */,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        DictionarySourcePtr source_ptr,
        ContextPtr /* global_context */,
        bool /* created_from_ddl */) -> DictionaryPtr
    {
        /// The structure must be a complex key with a single String element, followed by a class id
        /// attribute and a count attribute.
        if (!dict_struct.key || dict_struct.key->size() != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary must have exactly one complex key column (the n-gram text)");

        if (dict_struct.attributes.size() < 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary must have at least two attributes: class_id (UInt32) and count (UInt64)");

        const String layout_prefix = config_prefix + ".layout.naive_bayes";

        if (!config.has(layout_prefix + ".n"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary layout requires 'n' parameter (n-gram size)");

        if (!config.has(layout_prefix + ".mode"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary layout requires 'mode' parameter (byte/codepoint/token)");

        const auto n = static_cast<UInt32>(config.getUInt(layout_prefix + ".n"));
        if (n == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: n-gram size 'n' must be greater than 0");

        const String mode = config.getString(layout_prefix + ".mode");
        if (mode != "byte" && mode != "codepoint" && mode != "token")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: mode must be 'byte', 'codepoint', or 'token', got '{}'",
                mode);

        const double alpha = config.getDouble(layout_prefix + ".alpha", 1.0);
        if (alpha <= 0.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: alpha must be greater than 0");

        const String priors_mode_str = config.getString(layout_prefix + ".priors_mode", "uniform");
        PriorsMode priors_mode = PriorsMode::Uniform;
        std::map<UInt32, double> explicit_priors;
        if (priors_mode_str == "uniform")
        {
            priors_mode = PriorsMode::Uniform;
        }
        else if (priors_mode_str == "proportional")
        {
            priors_mode = PriorsMode::Proportional;
        }
        else if (priors_mode_str == "explicit")
        {
            priors_mode = PriorsMode::Explicit;
            if (!config.has(layout_prefix + ".priors"))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary: priors_mode 'explicit' requires a 'priors' parameter, e.g. priors '0=0.6,1=0.4'");
            explicit_priors = parseExplicitPriors(config.getString(layout_prefix + ".priors"));
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: priors_mode must be 'uniform', 'proportional', or 'explicit', got '{}'",
                priors_mode_str);
        }

        const bool store_source = config.getBool(layout_prefix + ".store_source", false);

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        NaiveBayesDictionary::Configuration cfg
        {
            .n = n,
            .mode = mode,
            .alpha = alpha,
            .priors_mode = priors_mode,
            .explicit_priors = std::move(explicit_priors),
            .store_source = store_source,
            .dict_lifetime = dict_lifetime,
        };

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        return std::make_unique<NaiveBayesDictionary>(dict_id, dict_struct, std::move(source_ptr), std::move(cfg));
    };

    /// This layout supports complex keys only; there is no simple-key variant.
    factory.registerLayout("naive_bayes", create_layout, /* is_layout_complex= */ true, /* has_layout_complex= */ false);
}

}
