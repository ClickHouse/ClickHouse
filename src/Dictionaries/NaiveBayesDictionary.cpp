#include <Dictionaries/NaiveBayesDictionary.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <Dictionaries/NaiveBayesTrainer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>

#include <cmath>
#include <limits>
#include <unordered_set>


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
    if (priors_str.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Explicit priors specification is empty");

    std::map<UInt32, double> priors;
    double total = 0.0;

    /// Iterate over every comma-separated entry, including the one after a trailing comma (which is empty
    /// and rejected below), so that a malformed `0=0.5,1=0.5,` is not silently accepted.
    size_t pos = 0;
    while (true)
    {
        const size_t comma = priors_str.find(',', pos);
        const size_t entry_end = (comma == String::npos) ? priors_str.size() : comma;
        const std::string_view entry(priors_str.data() + pos, entry_end - pos);

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

        if (!std::isfinite(prob) || prob <= 0.0 || prob > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prior probability for class {} must be a finite number in (0, 1], got {}", class_id, prob);

        if (!priors.emplace(class_id, prob).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate prior for class {}", class_id);

        total += prob;

        if (comma == String::npos)
            break;
        pos = comma + 1;
    }

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
    /// string; the class and count attributes are located by the indices resolved from `class_attribute`.
    const size_t key_size = dict_struct.getKeysSize();

    MutableColumnPtr ngram_acc;
    MutableColumnPtr class_id_acc;
    MutableColumnPtr count_acc;

    BlockIO io = source_ptr->loadAll();

    /// Validate that the source n-grams match the configured n. A model whose n-grams have a different
    /// arity would otherwise load silently and then classify everything by the prior, so reject it here.
    /// Checking a bounded sample of the first rows keeps loading cheap while catching a misconfigured
    /// n or mode.
    const size_t ngram_check_limit = 1000;
    size_t validated_ngrams = 0;

    io.executeWithCallbacks([&]()
    {
        DictionaryPipelineExecutor executor(io.pipeline, false);
        io.pipeline.setConcurrencyControl(false);

        Block block;
        while (executor.pull(block))
        {
            const size_t rows = block.rows();
            const auto & ngram_col = block.safeGetByPosition(0).column;
            const auto & class_id_col = block.safeGetByPosition(key_size + configuration.class_index).column;
            const auto & count_col = block.safeGetByPosition(key_size + configuration.count_index).column;

            for (size_t i = 0; i < rows; ++i)
            {
                const std::string_view ngram_sv = ngram_col->getDataAt(i);
                const UInt64 raw_class_id = class_id_col->getUInt(i);
                if (raw_class_id > std::numeric_limits<UInt32>::max())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "NaiveBayes dictionary class id {} exceeds the supported maximum of {}",
                        raw_class_id, std::numeric_limits<UInt32>::max());
                const auto class_id = static_cast<UInt32>(raw_class_id);
                const UInt64 count = count_col->getUInt(i);
                std::visit([&](auto & t)
                {
                    if (validated_ngrams < ngram_check_limit)
                    {
                        const size_t tokens = t.tokenCount(ngram_sv);
                        if (tokens != configuration.n)
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "NaiveBayes dictionary: source n-gram '{}' resolves to {} token(s) for mode '{}', but the "
                                "layout specifies n = {}. The source n-grams must match the configured size and mode.",
                                ngram_sv, tokens, configuration.mode, configuration.n);
                        ++validated_ngrams;
                    }
                    t.addNgram(class_id, ngram_sv, count);
                }, trainer);
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

    /// Finalizing the trainer computes the priors, compiles the flat model, and yields the immutable model.
    model_variant.emplace(std::visit(
        [&](auto & t) -> ModelVariant { return ModelVariant{t.finalize(configuration.priors_mode, configuration.explicit_priors)}; },
        trainer));

    if (configuration.store_source && ngram_acc)
    {
        source_ngram_column = std::move(ngram_acc);
        source_class_id_column = std::move(class_id_acc);
        source_count_column = std::move(count_acc);
    }

    element_count = std::visit([](const auto & model) { return model.getElementCount(); }, *model_variant);
    bytes_allocated = std::visit([](const auto & model) { return model.getAllocatedBytes(); }, *model_variant);

    /// Retaining the source rows for `store_source` costs memory too, so include it in the reported
    /// footprint instead of reporting only the model.
    if (configuration.store_source && source_ngram_column)
        bytes_allocated += source_ngram_column->allocatedBytes()
            + source_class_id_column->allocatedBytes()
            + source_count_column->allocatedBytes();

    LOG_INFO(log, "Loaded NaiveBayes dictionary with {} n-grams, {} bytes allocated", element_count, bytes_allocated);
}


ColumnPtr NaiveBayesDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & attribute_type,
    const Columns & key_columns,
    const DataTypes & /* key_types */,
    DefaultOrFilter /* default_or_filter */) const
{
    /// Only the class attribute is computable (it is the predicted class). The count attribute describes the
    /// training source and has no meaning as a per-input prediction.
    const auto & class_attribute_name = dict_struct.attributes[configuration.class_index].name;
    if (attribute_name != class_attribute_name)
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "NaiveBayes dictionary only supports querying attribute '{}' (the predicted class), got '{}'",
            class_attribute_name,
            attribute_name);

    const auto * string_col = typeid_cast<const ColumnString *>(key_columns.front().get());
    if (!string_col)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary key must be a String column");

    const size_t rows = string_col->size();

    /// dictGet must return the declared class-attribute type. The predicted class ids are the original source
    /// values, so they fit whichever unsigned width was declared; build the result column in that type.
    auto classify_as = [&]<typename T>() -> ColumnPtr
    {
        auto column = ColumnVector<T>::create(rows);
        auto & data = column->getData();
        visitModel([&](const auto & model)
        {
            NaiveBayesScratch scratch;
            for (size_t i = 0; i < rows; ++i)
                data[i] = static_cast<T>(model.classify(string_col->getDataAt(i), scratch));
        });
        return column;
    };

    const WhichDataType which(attribute_type);
    ColumnPtr result;
    if (which.isUInt8())
        result = classify_as.operator()<UInt8>();
    else if (which.isUInt16())
        result = classify_as.operator()<UInt16>();
    else if (which.isUInt32())
        result = classify_as.operator()<UInt32>();
    else
        result = classify_as.operator()<UInt64>();

    query_count.fetch_add(rows, std::memory_order_relaxed);

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

    return result;
}


Pipe NaiveBayesDictionary::read(const Names & column_names, size_t /* max_block_size */, size_t /* num_streams */) const
{
    if (!configuration.store_source)
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Set `store_source` to true in the NAIVE_BAYES layout to support reading the training data back from the dictionary");

    /// With store_source set, a successful load always populates these columns (an empty source throws
    /// RECEIVED_EMPTY_DATA before the dictionary becomes usable), so they are never null here.
    chassert(source_ngram_column && source_class_id_column && source_count_column);

    const auto & key_attribute = (*dict_struct.key)[0];

    ColumnsWithTypeAndName result_columns;
    result_columns.reserve(column_names.size());
    for (const auto & column_name : column_names)
    {
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = column_name;

        if (column_name == key_attribute.name)
        {
            column_with_type.column = source_ngram_column;
            column_with_type.type = key_attribute.type;
        }
        else
        {
            const auto & attribute = dict_struct.getAttribute(column_name);
            if (column_name == dict_struct.attributes.front().name)
                column_with_type.column = source_class_id_column;
            else
                column_with_type.column = source_count_column;
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

        if (dict_struct.attributes.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary must have exactly two attributes: the class label and the count, both unsigned integers");

        /// The key holds the n-gram text; the two attributes are the class label and the count (which is which is
        /// resolved below from the `class_attribute` parameter). Both must be unsigned integers: the source columns
        /// are coerced to these declared types, so this makes the `getUInt` reads in loadData well defined.
        const auto & key_type = (*dict_struct.key)[0].type;
        if (!isString(key_type))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary key must be String, got {}", key_type->getName());

        for (size_t i = 0; i < 2; ++i)
        {
            const auto & attribute = dict_struct.attributes[i];
            if (!WhichDataType(attribute.type).isNativeUInt())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary attribute '{}' must be an unsigned integer (UInt8/16/32/64), got {}",
                    attribute.name,
                    attribute.type->getName());
        }

        const String layout_prefix = config_prefix + ".layout.naive_bayes";

        /// Reject unknown layout parameters so typos (for example `priors_mod`) are caught at creation instead
        /// of being silently ignored.
        static const std::unordered_set<std::string_view> known_layout_keys{
            "n", "mode", "alpha", "priors_mode", "priors", "store_source", "class_attribute"};
        Poco::Util::AbstractConfiguration::Keys layout_keys;
        config.keys(layout_prefix, layout_keys);
        for (const auto & key : layout_keys)
            if (!known_layout_keys.contains(key))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary: unknown layout parameter '{}'. Allowed: n, mode, alpha, priors_mode, priors, store_source, class_attribute",
                    key);

        if (!config.has(layout_prefix + ".n"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary layout requires 'n' parameter (n-gram size)");

        if (!config.has(layout_prefix + ".mode"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary layout requires 'mode' parameter (byte/codepoint/token)");

        const UInt64 n_raw = config.getUInt64(layout_prefix + ".n");
        if (n_raw == 0 || n_raw > std::numeric_limits<UInt32>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: n-gram size 'n' must be between 1 and {}, got {}",
                std::numeric_limits<UInt32>::max(), n_raw);
        const auto n = static_cast<UInt32>(n_raw);

        const String mode = config.getString(layout_prefix + ".mode");
        if (mode != "byte" && mode != "codepoint" && mode != "token")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: mode must be 'byte', 'codepoint', or 'token', got '{}'",
                mode);

        const double alpha = config.getDouble(layout_prefix + ".alpha", 1.0);
        if (!std::isfinite(alpha) || alpha <= 0.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: alpha must be a finite number greater than 0");

        const String priors_mode_str = config.getString(layout_prefix + ".priors_mode", "proportional");
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

        /// `priors` is consulted only in explicit mode; reject it otherwise so that a mistaken
        /// `priors 'proportional'` (meaning `priors_mode 'proportional'`) is not silently ignored.
        if (priors_mode != PriorsMode::Explicit && config.has(layout_prefix + ".priors"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: 'priors' is only valid with priors_mode 'explicit'");

        const bool store_source = config.getBool(layout_prefix + ".store_source", false);

        /// `class_attribute` names which of the two attributes is the class label; the other is the count. Requiring
        /// it, rather than assuming an attribute order, makes each column's role explicit, so reordering the attribute
        /// declarations cannot silently swap the class and the count.
        if (!config.has(layout_prefix + ".class_attribute"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary layout requires a 'class_attribute' parameter naming the class label column, "
                "e.g. class_attribute 'class_id'");
        const String class_attribute = config.getString(layout_prefix + ".class_attribute");

        size_t class_index = dict_struct.attributes.size();
        for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
            if (dict_struct.attributes[i].name == class_attribute)
                class_index = i;
        if (class_index == dict_struct.attributes.size())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: class_attribute '{}' does not name any attribute; the attributes are '{}' and '{}'",
                class_attribute,
                dict_struct.attributes[0].name,
                dict_struct.attributes[1].name);
        const size_t count_index = 1 - class_index;

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        NaiveBayesDictionary::Configuration cfg
        {
            .n = n,
            .mode = mode,
            .alpha = alpha,
            .priors_mode = priors_mode,
            .explicit_priors = std::move(explicit_priors),
            .store_source = store_source,
            .class_index = class_index,
            .count_index = count_index,
            .dict_lifetime = dict_lifetime,
        };

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        return std::make_unique<NaiveBayesDictionary>(dict_id, dict_struct, std::move(source_ptr), std::move(cfg));
    };

    /// This layout supports complex keys only; there is no simple-key variant.
    factory.registerLayout("naive_bayes", create_layout, /* is_layout_complex= */ true, /* has_layout_complex= */ false);
}

}
