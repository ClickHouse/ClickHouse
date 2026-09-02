#include <Dictionaries/NaiveBayesDictionary.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/NaiveBayesTrainer.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/castColumn.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/logger_useful.h>

#include <charconv>
#include <cmath>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
extern const int TYPE_MISMATCH;
}

namespace
{

/// Reads the explicit priors from the structured `priors` layout parameter: repeated `prior` elements,
/// each holding a `class` id and a `probability`. A DDL definition produces them from a collection
/// literal such as `priors [(0, 0.6), (1, 0.4)]`; an XML definition spells the `prior` elements out
/// directly.
MapWithMemoryTracking<UInt32, double> parseExplicitPriors(const Poco::Util::AbstractConfiguration & config, const String & priors_prefix)
{
    Poco::Util::AbstractConfiguration::Keys prior_keys;
    config.keys(priors_prefix, prior_keys);

    if (prior_keys.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "NaiveBayes dictionary: the explicit priors must be a non-empty collection of (class, probability) pairs, "
            "e.g. priors [(0, 0.6), (1, 0.4)]");

    MapWithMemoryTracking<UInt32, double> priors;
    double total = 0.0;

    for (const auto & prior_key : prior_keys)
    {
        if (prior_key != "prior" && !prior_key.starts_with("prior["))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: unexpected element '{}' in priors, expected repeated 'prior' elements",
                prior_key);

        const String prior_prefix = priors_prefix + "." + prior_key;
        if (!config.has(prior_prefix + ".class") || !config.has(prior_prefix + ".probability"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: each prior must contain a 'class' id and a 'probability'");

        const String class_str = trim(config.getString(prior_prefix + ".class"), isWhitespaceASCII);
        const String prob_str = trim(config.getString(prior_prefix + ".probability"), isWhitespaceASCII);

        /// Parse the class id directly into UInt32 with overflow checking. parse<>/readIntText silently wraps a
        /// value past the type's range onto a different valid class, so use from_chars, which reports overflow
        /// and rejects any non-digit input. The model represents class ids as 32-bit values.
        UInt32 class_id = 0;
        const char * const class_begin = class_str.data();
        const char * const class_end = class_begin + class_str.size();
        const auto [class_ptr, class_ec] = std::from_chars(class_begin, class_end, class_id);
        if (class_ec == std::errc::result_out_of_range)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: the priors class id '{}' exceeds the supported maximum of {}",
                class_str,
                std::numeric_limits<UInt32>::max());
        if (class_ec != std::errc{} || class_ptr != class_end)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: the priors class id '{}' is not a non-negative integer",
                class_str);

        double prob = 0.0;
        try
        {
            prob = parse<Float64>(prob_str);
        }
        catch (const Exception &)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: the prior probability '{}' for class {} is not a number",
                prob_str,
                class_id);
        }

        if (!std::isfinite(prob) || prob <= 0.0 || prob > 1.0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: the prior probability for class {} must be a finite number in (0, 1], got {}",
                class_id,
                prob);

        if (!priors.emplace(class_id, prob).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: duplicate prior for class {}", class_id);

        total += prob;
    }

    if (std::fabs(total - 1.0) > 1e-6)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: the prior probabilities must sum to 1.0, got {}", total);

    return priors;
}

/// A copy of the dictionary structure with the class and count attributes widened to UInt64.
/// The source pipeline uses it to deliver the source values at their full width, so the training
/// loop can validate them against the declared attribute types instead of receiving them already
/// wrapped by the narrowing cast.
DictionaryStructure widenAttributesToUInt64(const DictionaryStructure & dict_struct)
{
    DictionaryStructure widened = dict_struct;
    widened.attributes.clear();

    const auto wide_type = std::make_shared<DataTypeUInt64>();
    for (const auto & attribute : dict_struct.attributes)
        widened.attributes.emplace_back(DictionaryAttribute{
            .name = attribute.name,
            .type = wide_type,
            .type_serialization = wide_type->getDefaultSerialization(),
            .expression = attribute.expression,
            .null_value = attribute.null_value,
            .underlying_type = AttributeUnderlyingType::UInt64,
            .hierarchical = attribute.hierarchical,
            .bidirectional = attribute.bidirectional,
            .injective = attribute.injective,
            .is_object_id = attribute.is_object_id,
            .is_nullable = attribute.is_nullable});

    return widened;
}

/// Highest value representable in the declared unsigned attribute type.
UInt64 maxValueOfDeclaredType(const DataTypePtr & type)
{
    WhichDataType which(type);
    if (which.isUInt8())
        return std::numeric_limits<UInt8>::max();
    if (which.isUInt16())
        return std::numeric_limits<UInt16>::max();
    if (which.isUInt32())
        return std::numeric_limits<UInt32>::max();
    return std::numeric_limits<UInt64>::max();
}

}


NaiveBayesDictionary::NaiveBayesDictionary(
    const StorageID & dict_id_, const DictionaryStructure & dict_struct_, DictionarySourcePtr source_ptr_, Configuration configuration_)
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
    using Trainer = std::variant<NaiveBayesTrainer<BytePolicy>, NaiveBayesTrainer<CodePointPolicy>, NaiveBayesTrainer<TokenPolicy>>;

    Trainer trainer_variant = [&]() -> Trainer
    {
        switch (configuration.mode)
        {
            case TokenizerMode::Byte:
                return Trainer{
                    std::in_place_type<NaiveBayesTrainer<BytePolicy>>,
                    configuration.n,
                    configuration.alpha,
                    configuration.start_token,
                    configuration.end_token};
            case TokenizerMode::CodePoint:
                return Trainer{
                    std::in_place_type<NaiveBayesTrainer<CodePointPolicy>>,
                    configuration.n,
                    configuration.alpha,
                    configuration.start_token,
                    configuration.end_token};
            case TokenizerMode::Token:
                return Trainer{
                    std::in_place_type<NaiveBayesTrainer<TokenPolicy>>,
                    configuration.n,
                    configuration.alpha,
                    configuration.start_token,
                    configuration.end_token};
        }
        UNREACHABLE();
    }();

    /// The block places the key columns first and the attribute columns after them. The key is the n-gram
    /// string; the class and count attributes are located by the indices resolved from `class_attribute`.
    const size_t key_size = dict_struct.getKeysSize();

    /// The source delivers the class and count columns widened to UInt64 (see the source creation in
    /// `registerDictionaryNaiveBayes`), so out-of-range values arrive unwrapped and can be validated
    /// against the declared attribute types here.
    const auto & class_attribute = dict_struct.attributes[configuration.class_index];
    const auto & count_attribute = dict_struct.attributes[configuration.count_index];
    const UInt64 class_id_declared_max = maxValueOfDeclaredType(class_attribute.type);
    const UInt64 count_declared_max = maxValueOfDeclaredType(count_attribute.type);

    MutableColumnPtr ngram_accumulator;
    MutableColumnPtr class_id_accumulator;
    MutableColumnPtr count_accumulator;

    BlockIO io = source_ptr->loadAll();

    /// Stream the source rows into the trainer, validating each n-gram against the configured n and mode. A
    /// malformed n-gram — one with the wrong arity, or invalid UTF-8 in codepoint mode — can never be produced
    /// by the tokenizer at query time, so it is rejected here.
    io.executeWithCallbacks(
        [&]()
        {
            DictionaryPipelineExecutor executor(io.pipeline, false);
            io.pipeline.setConcurrencyControl(false);

            /// TODO (nihalzp): We are processing it single threaded and one chunk at a time. This should be okay because
            /// the training data is already pre-aggregated and is supposed to be small for most cases. However, we can theoretically
            /// parallelize this by having multiple threads processing different chunks and a smart merge of processed intermediate data.
            /// This could be useful if the training data is huge like millions of rows.
            Block block;
            while (executor.pull(block))
            {
                const size_t rows = block.rows();
                const auto & ngram_col = block.safeGetByPosition(0).column;
                const auto & class_id_col = block.safeGetByPosition(key_size + configuration.class_index).column;
                const auto & count_col = block.safeGetByPosition(key_size + configuration.count_index).column;

                for (size_t i = 0; i < rows; ++i)
                {
                    /// TODO (nihalzp): Currently, it does virtual call every row. We can optimize by downcasting
                    /// each column to its concrete type once per block and reading the raw buffer instead of the
                    /// per-row getDataAt/getUInt.
                    const std::string_view ngram_sv = ngram_col->getDataAt(i);
                    /// The declared-type check covers every type up to UInt32; the model-limit check
                    /// is reachable only when the class attribute is declared UInt64, since the model
                    /// represents class ids as 32-bit values.
                    const UInt64 raw_class_id = class_id_col->getUInt(i);
                    if (raw_class_id > class_id_declared_max)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "NaiveBayes dictionary: the source class id {} does not fit the declared type {} of attribute '{}'",
                            raw_class_id,
                            class_attribute.type->getName(),
                            class_attribute.name);
                    if (raw_class_id > std::numeric_limits<UInt32>::max())
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "NaiveBayes dictionary class id {} exceeds the supported maximum of {}",
                            raw_class_id,
                            std::numeric_limits<UInt32>::max());
                    const auto class_id = static_cast<UInt32>(raw_class_id);
                    const UInt64 count = count_col->getUInt(i);
                    if (count > count_declared_max)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "NaiveBayes dictionary: the source count {} does not fit the declared type {} of attribute '{}'",
                            count,
                            count_attribute.type->getName(),
                            count_attribute.name);
                    std::visit(
                        [&](auto & trainer)
                        {
                            const auto prepared = trainer.prepareNgram(ngram_sv);
                            if (!prepared.valid)
                                throw Exception(
                                    ErrorCodes::BAD_ARGUMENTS,
                                    "NaiveBayes dictionary: source n-gram '{}' is not valid UTF-8 for mode '{}'. Use mode "
                                    "'byte' for arbitrary byte sequences.",
                                    ngram_sv,
                                    toString(configuration.mode));
                            if (prepared.token_count != configuration.n)
                                throw Exception(
                                    ErrorCodes::BAD_ARGUMENTS,
                                    "NaiveBayes dictionary: source n-gram '{}' resolves to {} token(s) for mode '{}', but the "
                                    "layout specifies n = {}. The source n-grams must match the configured size and mode.",
                                    ngram_sv,
                                    prepared.token_count,
                                    toString(configuration.mode),
                                    configuration.n);
                            trainer.addNgram(class_id, prepared.key, count);
                        },
                        trainer_variant);
                }

                if (configuration.store_source)
                {
                    /// A source column may arrive in sparse serialization; inserting it into a dense accumulator
                    /// would fail, so materialize each column to full before retaining it. The class and count
                    /// columns arrive widened to UInt64; cast each block back to the declared attribute types
                    /// (every row of the block was validated above to fit) so the retained rows keep the
                    /// declared schema for `read` without accumulating the widened columns.
                    const auto ngram_full = ngram_col->convertToFullColumnIfSparse();
                    const auto wide_type = std::make_shared<DataTypeUInt64>();
                    const auto class_id_declared
                        = castColumn({class_id_col->convertToFullColumnIfSparse(), wide_type, ""}, class_attribute.type);
                    const auto count_declared = castColumn({count_col->convertToFullColumnIfSparse(), wide_type, ""}, count_attribute.type);
                    if (!ngram_accumulator)
                    {
                        ngram_accumulator = ngram_full->cloneEmpty();
                        class_id_accumulator = class_id_declared->cloneEmpty();
                        count_accumulator = count_declared->cloneEmpty();
                    }
                    ngram_accumulator->insertRangeFrom(*ngram_full, 0, rows);
                    class_id_accumulator->insertRangeFrom(*class_id_declared, 0, rows);
                    count_accumulator->insertRangeFrom(*count_declared, 0, rows);
                }
            }
        });

    /// Finalizing the trainer computes the priors, compiles the flat model, and yields the immutable model.
    model_variant.emplace(
        std::visit(
            [&](auto & trainer) -> ModelVariant
            { return ModelVariant{trainer.finalize(configuration.priors_mode, configuration.explicit_priors)}; },
            trainer_variant));

    if (configuration.store_source && ngram_accumulator)
    {
        source_ngram_column = std::move(ngram_accumulator);
        source_class_id_column = std::move(class_id_accumulator);
        source_count_column = std::move(count_accumulator);
    }

    element_count = visitModel([](const auto & model) { return model.getElementCount(); });
    bytes_allocated = visitModel([](const auto & model) { return model.getAllocatedBytes(); });

    /// Retaining the source rows for `store_source` costs memory too, so include it in the reported
    /// footprint instead of reporting only the model.
    if (configuration.store_source && source_ngram_column)
        bytes_allocated
            += source_ngram_column->allocatedBytes() + source_class_id_column->allocatedBytes() + source_count_column->allocatedBytes();

    LOG_INFO(log, "Loaded NaiveBayes dictionary with {} n-grams, {} bytes allocated", element_count, bytes_allocated);
}


ColumnPtr NaiveBayesDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & attribute_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    DefaultOrFilter default_or_filter) const
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

    dict_struct.validateKeyTypes(key_types);

    const auto * string_col = typeid_cast<const ColumnString *>(key_columns.front().get());
    if (!string_col)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "NaiveBayes dictionary key must be a String column");

    const size_t rows = string_col->size();

    if (std::holds_alternative<RefFilter>(default_or_filter))
        std::get<RefFilter>(default_or_filter).get().assign(rows, static_cast<UInt8>(0));

    /// dictGet must return the declared class-attribute type. The predicted class ids are the source class
    /// values, so they fit whichever unsigned width is declared; build the result column in that type.
    auto classify_as = [&]<typename T>() -> ColumnPtr
    {
        auto column = ColumnVector<T>::create(rows);
        auto & data = column->getData();
        visitModel(
            [&](const auto & model)
            {
                NaiveBayesScratch scratch;
                for (size_t i = 0; i < rows; ++i)
                    data[i] = static_cast<T>(model.classify(string_col->getDataAt(i), scratch));
            });
        return column;
    };

    query_count.fetch_add(rows, std::memory_order_relaxed);

    const WhichDataType which(attribute_type);
    if (which.isUInt8())
        return classify_as.operator()<UInt8>();
    if (which.isUInt16())
        return classify_as.operator()<UInt16>();
    if (which.isUInt32())
        return classify_as.operator()<UInt32>();
    return classify_as.operator()<UInt64>();
}


ColumnUInt8::Ptr NaiveBayesDictionary::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    dict_struct.validateKeyTypes(key_types);

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

            /// Pick the stored class or count column by the resolved class attribute.
            if (column_name == dict_struct.attributes[configuration.class_index].name)
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
    auto create_layout = [](const std::string & full_name,
                            const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            DictionarySourcePtr source_ptr,
                            ContextPtr global_context,
                            bool created_from_ddl) -> DictionaryPtr
    {
        /// The structure must be a complex key with a single String element, followed by two unsigned-integer
        /// attributes: a class label and a count.
        if (!dict_struct.key || dict_struct.key->size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary must have exactly one complex key column (the n-gram text)");

        if (dict_struct.attributes.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary must have exactly two attributes: the class label and the count, both unsigned integers");

        /// The key holds the n-gram text; the two attributes are the class label and the count, and the
        /// `class_attribute` parameter (resolved below) says which is which. Both must be unsigned integers.
        const auto & key_type = (*dict_struct.key)[0].type;
        if (!isString(key_type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary key must be String, got {}", key_type->getName());

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
        static const UnorderedSetWithMemoryTracking<std::string_view> known_layout_keys{
            "n", "mode", "alpha", "priors_mode", "priors", "store_source", "class_attribute", "start_token", "end_token"};
        Poco::Util::AbstractConfiguration::Keys layout_keys;
        config.keys(layout_prefix, layout_keys);
        for (const auto & key : layout_keys)
            if (!known_layout_keys.contains(key))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary: unknown layout parameter '{}'. Allowed: n, mode, alpha, priors_mode, priors, store_source, "
                    "class_attribute, start_token, end_token",
                    key);

        if (!config.has(layout_prefix + ".n"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary layout requires 'n' parameter (n-gram size)");

        if (!config.has(layout_prefix + ".mode"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary layout requires 'mode' parameter (byte/codepoint/token)");

        const UInt64 n_raw = config.getUInt64(layout_prefix + ".n");
        if (n_raw == 0 || n_raw > MAX_NGRAM_SIZE)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: n-gram size 'n' must be in range [1, {}], got {}",
                MAX_NGRAM_SIZE,
                n_raw);
        const auto n = static_cast<UInt32>(n_raw);

        const TokenizerMode mode = parseTokenizerMode(config.getString(layout_prefix + ".mode"));

        /// Padding is opt-in and per-side: by default the input is tokenized as-is. When `start_token` and/or
        /// `end_token` is given, that side of the query input is padded with (n - 1) copies of it, exactly as a
        /// training pipeline must have padded the source n-grams. The two are independent — set one, both, or
        /// neither — and an empty value means that side is not padded, the same as omitting it. Raw bytes cannot
        /// travel through the dictionary config, so byte/codepoint tokens are numbers (resolved to bytes here)
        /// while token mode takes the literal token.
        auto resolve_token = [&](std::string_view parameter_name) -> String
        {
            const String raw = config.getString(layout_prefix + "." + String(parameter_name), "");
            return raw.empty() ? String{} : parsePaddingToken(raw, mode, parameter_name);
        };
        String start_token = resolve_token("start_token");
        String end_token = resolve_token("end_token");

        const double alpha = config.getDouble(layout_prefix + ".alpha", 1.0);
        if (!std::isfinite(alpha) || alpha <= 0.0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: alpha must be a finite number greater than 0, got {}", alpha);

        const String priors_mode_str = config.getString(layout_prefix + ".priors_mode", "proportional");
        PriorsMode priors_mode = PriorsMode::Uniform;
        MapWithMemoryTracking<UInt32, double> explicit_priors;
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
                    "NaiveBayes dictionary: priors_mode 'explicit' requires a 'priors' parameter, e.g. priors [(0, 0.6), (1, 0.4)]");
            explicit_priors = parseExplicitPriors(config, layout_prefix + ".priors");
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: priors_mode must be 'uniform', 'proportional', or 'explicit', got '{}'",
                priors_mode_str);
        }

        /// `priors` is consulted only in explicit mode; reject it otherwise.
        if (priors_mode != PriorsMode::Explicit && config.has(layout_prefix + ".priors"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: 'priors' is only valid with priors_mode 'explicit'");

        const bool store_source = config.getBool(layout_prefix + ".store_source", false);

        /// `class_attribute` names which of the two attributes is the class label; the other is the count.
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

        /// The source pipeline casts every column to the declared attribute types with a plain wrapping
        /// cast, so a source class id or count that does not fit the declared type would be silently
        /// truncated before this dictionary could see it. Recreate the source with both attributes
        /// widened to UInt64 — a lossless conversion for every unsigned source column — and let the
        /// training loop validate the true values against the declared types.
        source_ptr = DictionarySourceFactory::instance().create(
            full_name,
            config,
            config_prefix + ".source",
            widenAttributesToUInt64(dict_struct),
            global_context,
            config.getString(config_prefix + ".database", ""),
            created_from_ddl);

        NaiveBayesDictionary::Configuration cfg{
            .n = n,
            .mode = mode,
            .alpha = alpha,
            .start_token = std::move(start_token),
            .end_token = std::move(end_token),
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

    factory.registerLayout(
        "naive_bayes",
        create_layout,
        /* is_layout_complex= */ true,
        /* has_layout_complex= */ false,
        Documentation{
            .description = R"DOCS_MD(
# Naive Bayes dictionaries

The `naive_bayes` (`NAIVE_BAYES`) dictionary classifies text with a multinomial [Naive Bayes](https://en.wikipedia.org/wiki/Naive_Bayes_classifier) model, the standard event model for text: it scores each class by how often the input's n-grams appear in it. You give it a table of per-class **n-gram counts**, which it compiles into a model once, at load time, then uses to classify any text you pass in.

It is suited to fast, lightweight text classification such as sentiment analysis, topic or spam labelling, and language or script detection.

You query the dictionary with one of three functions:

- [`naiveBayesClassifier`](/reference/functions/regular-functions/machine-learning-functions#naiveBayesClassifier) returns the predicted class id.
- [`naiveBayesClassifierWithProb`](/reference/functions/regular-functions/machine-learning-functions#naiveBayesClassifierWithProb) returns the predicted class with its probability.
- [`naiveBayesClassifierWithAllProbs`](/reference/functions/regular-functions/machine-learning-functions#naiveBayesClassifierWithAllProbs) returns every class with its probability.

A plain [`dictGet`](/reference/functions/regular-functions/ext-dict-functions#dictGet) classifies too (see [Notes](#notes)). One more function, [`naiveBayesNgrams`](/reference/functions/regular-functions/splitting-merging-functions#naiveBayesNgrams), does not classify — it splits text into n-grams the same way the dictionary does, so you can build the training data from raw text (see [Build training data from raw text](#build-training-data-from-raw-text)).

## Quickstart {#quickstart}

Here we build a token-mode, unigram (`n = 1`) model for sentiment analysis.

**1. Create a source table** of per-class n-gram counts:

```sql
CREATE TABLE training_data (class_id UInt32, ngram String, count UInt64)
ENGINE = MergeTree ORDER BY (class_id, ngram);
```

**2. Insert training data** — single words (unigrams) and how often each occurs in the positive (`1`) and negative (`0`) class:

```sql
INSERT INTO training_data VALUES
    (1,'good',10),(1,'great',8),(1,'excellent',6),(1,'love',7),(1,'happy',5),
    (1,'amazing',4),(1,'wonderful',3),(1,'best',3),(1,'fantastic',2),(1,'nice',4),
    (0,'bad',10),(0,'terrible',8),(0,'awful',6),(0,'hate',7),(0,'worst',5),
    (0,'horrible',4),(0,'poor',3),(0,'disappointing',3),(0,'ugly',2),(0,'sad',4);
```

**3. Create the dictionary** with the `NAIVE_BAYES` layout:

```sql
CREATE DICTIONARY sentiment (ngram String, class_id UInt32, count UInt64)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'training_data'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);
```

`PRIMARY KEY ngram` makes the `ngram` column the key — but for a `NAIVE_BAYES` dictionary this "key" is the text you pass in to classify, not a stored value you look up (see [Dictionary structure](#dictionary-structure)). The `LAYOUT` configures the model: `class_attribute 'class_id'` marks `class_id` as the class label (so the other attribute, `count`, is the per-class occurrence count), `n 1` uses unigrams, and `mode 'token'` splits text into whitespace-delimited words (see [Layout parameters](#layout-parameters)).

**4. Classify** — `naiveBayesClassifier` returns the class id:

```sql
SELECT naiveBayesClassifier('sentiment', 'this is great') as predicted_class;
```

```response
   ┌─predicted_class─┐
1. │               1 │
   └─────────────────┘
```

`1` maps to the positive class, based on the training data we inserted in step 2.

```sql
SELECT naiveBayesClassifier('sentiment', 'this is terrible') as predicted_class;
```

```response
   ┌─predicted_class─┐
1. │               0 │
   └─────────────────┘
```

Likewise, `0` maps to the negative class.

The same result via `dictGet`:

```sql
SELECT dictGet('sentiment', 'class_id', 'this is great') as predicted_class;
```

```response
   ┌─predicted_class─┐
1. │               1 │
   └─────────────────┘
```

Get the probability of the prediction, or of every class:

```sql
SELECT naiveBayesClassifierWithProb('sentiment', 'amazing food but terrible service') as predicted_id_with_prob;
```

```response
   ┌─predicted_id_with_prob─────────────┐
1. │ {                                 ↴│
   │↳  "class_id": 0,                  ↴│
   │↳  "probability": 0.642857145060626↴│
   │↳}                                  │
   └────────────────────────────────────┘
```

The prediction is class `0` (negative) at probability `0.64`.

```sql
SELECT naiveBayesClassifierWithAllProbs('sentiment', 'amazing food but terrible service') as all_predicted_ids_with_probs;
```

```response
   ┌─all_predicted_ids_with_probs─────────┐
1. │ [{                                  ↴│
   │↳  "class_id": 0,                    ↴│
   │↳  "probability": 0.642857145060626  ↴│
   │↳},{                                 ↴│
   │↳  "class_id": 1,                    ↴│
   │↳  "probability": 0.35714285493937414↴│
   │↳}]                                   │
   └──────────────────────────────────────┘
```

`naiveBayesClassifierWithAllProbs` returns every class ordered from most to least likely, with probabilities that sum to `1.0` — here `0.64` for the negative class and `0.36` for the positive one.

## How it works {#how-it-works}

**Training (at load time).** Each source row is a `(n-gram, class, count)` observation. When the dictionary loads, the rows are compiled once into the model. Duplicate `(n-gram, class)` rows are summed, and rows with `count = 0` are ignored.

**Classifying (at query time).** To classify a string, the model:

1. Splits it into n-grams according to `mode` and `n` (see [Tokenization modes](#tokenization-modes)).
2. Scores each class by combining the class prior with how often the input's n-grams were seen in that class.
3. Ranks the classes by score. The top-scoring class is the prediction returned by `naiveBayesClassifier`; `naiveBayesClassifierWithProb` and `naiveBayesClassifierWithAllProbs` also return probabilities — for that class, or for all of them.

Two things affect the score for each class. The first is `alpha`, which is used for smoothing. Smoothing prevents the model from giving a class a score of zero just because one n-gram did not appear in that class during training. A smaller `alpha` makes the model rely more on the training data, so one class can get a much higher score than the others, but it can also make the model too sensitive when the training data is small or uneven. A larger `alpha` makes the n-gram counts matter less, so the scores for different classes become more similar. If `alpha` is very large, the n-gram information barely matters and the score is driven mostly by the class prior (described next).

The second is the class prior — what the model assumes about how likely each class is before it looks at the text. It acts as a starting score each class gets before any n-grams are considered, so a higher prior makes a class more likely to be predicted. How it is set depends on `priors_mode`. By default (`proportional`), a class with a larger total n-gram count in the training data starts with a higher score. With `uniform`, every class starts equal, so only the n-grams decide. With `explicit`, you set each class's starting point yourself. See [Prior modes](#prior-modes).

An n-gram that was never seen anywhere in the training data is ignored: it is not part of the model's vocabulary, so it does not help or hurt any class.

The algorithm follows the multinomial Naive Bayes model for text classification; see [Manning, Raghavan & Schütze, *Introduction to Information Retrieval*, ch. 13 (*Text Classification and Naive Bayes*)](https://nlp.stanford.edu/IR-book/html/htmledition/text-classification-and-naive-bayes-1.html).

## Dictionary structure {#dictionary-structure}

A `NAIVE_BAYES` dictionary has a fixed shape:

- The `PRIMARY KEY` is a single `String` column — the n-gram. At query time this "key" is the text you pass in to classify, not a stored lookup key.
- Alongside it, declare **exactly two unsigned-integer attributes**: the class label and the occurrence count. Class ids always use `UInt32` internally, so a class label must fit in `UInt32` (at most `4294967295`) even if you declare its attribute as `UInt64`. A larger value is rejected when the dictionary loads, not when you create it. The same applies to the declared types: a source class id or count that does not fit the declared attribute type fails the load instead of being silently truncated.
- The `class_attribute` layout parameter names which attribute is the class label; the other is automatically the count. The two attributes can be declared in either order.

The source table holds **pre-aggregated** counts: one row per `(n-gram, class)` with how many times that n-gram appeared in that class. You produce those counts by tokenizing your corpus and grouping the result, either in your own training pipeline or in ClickHouse from raw labelled text (see [Build training data from raw text](#build-training-data-from-raw-text)). The dictionary only consumes them.

**Updating the model.** Because the model is a dictionary backed by a table, retrain by updating the table and reloading:

```sql
INSERT INTO training_data VALUES (1, 'awesome', 5);
SYSTEM RELOAD DICTIONARY sentiment;
```

## Layout parameters {#layout-parameters}

| Parameter | Description | Example | Default |
| --- | --- | --- | --- |
| `class_attribute` | Name of the attribute that holds the class label; the other attribute is the count. | `'class_id'` | *Required* |
| `n` | N-gram size: `1` = unigrams, `2` = bigrams, `3` = trigrams, … (1–1024). | `2` | *Required* |
| `mode` | Tokenization method: `byte`, `codepoint`, or `token`. See [Tokenization modes](#tokenization-modes). | `'token'` | *Required* |
| `alpha` | Additive (Lidstone) smoothing for n-gram likelihoods; `alpha = 1` is Laplace smoothing (must be finite and `> 0`). | `0.5` | `1.0` |
| `priors_mode` | How class priors are determined: `uniform`, `proportional`, or `explicit`. See [Prior modes](#prior-modes). | `'uniform'` | `'proportional'` |
| `priors` | Explicit per-class priors: a collection of `(class, probability)` pairs. Valid only with `priors_mode 'explicit'`, where it is required; supplying it in any other mode is an error. Must sum to `1.0`. | `[(0, 0.6), (1, 0.4)]` | — |
| `store_source` | Retain the source rows so `SELECT * FROM dictionary` works. Roughly doubles memory. | `1` | `0` |
| `start_token` | Boundary token prepended `(n-1)` times to the input. See [Boundary tokens](#boundary-tokens-padding). | `'0x01'` / `'<s>'` | — (no padding) |
| `end_token` | Boundary token appended `(n-1)` times to the input. | `'0xFF'` / `'</s>'` | — (no padding) |

You can define the dictionary with `CREATE DICTIONARY` DDL (as in the quickstart above) or in an XML configuration file; see [Dictionary layouts](/reference/statements/create/dictionary/layouts/overview) for where that file goes. The example below sets every layout option so you can see them all — only `class_attribute`, `n`, and `mode` are required, and the table above gives the defaults for the rest. In a configuration file, the priors are written as repeated `prior` elements (one per class, as shown below), padding tokens for `byte` and `codepoint` are numbers (the config cannot carry raw bytes), and a `token` literal is XML-escaped where needed, so `<s>` becomes `&lt;s&gt;`.

<Tabs>
<Tab title="DDL">

```sql
CREATE DICTIONARY naive_bayes (ngram String, class_id UInt32, count UInt64)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'training_data'))
LAYOUT(NAIVE_BAYES(
    class_attribute 'class_id'
    n 2
    mode 'token'
    alpha 0.5
    priors_mode 'explicit'
    priors [(0, 0.6), (1, 0.4)]
    store_source 1
    start_token '<s>'
    end_token '</s>'
))
LIFETIME(3600);
```

</Tab>
<Tab title="Configuration file">

```xml
<dictionary>
    <name>naive_bayes</name>
    <structure>
        <key>
            <attribute>
                <name>ngram</name>
                <type>String</type>
            </attribute>
        </key>
        <attribute>
            <name>class_id</name>
            <type>UInt32</type>
            <null_value>0</null_value>
        </attribute>
        <attribute>
            <name>count</name>
            <type>UInt64</type>
            <null_value>0</null_value>
        </attribute>
    </structure>
    <source>
        <clickhouse>
            <table>training_data</table>
        </clickhouse>
    </source>
    <layout>
        <naive_bayes>
            <class_attribute>class_id</class_attribute>
            <n>2</n>
            <mode>token</mode>
            <alpha>0.5</alpha>
            <priors_mode>explicit</priors_mode>
            <priors>
                <prior>
                    <class>0</class>
                    <probability>0.6</probability>
                </prior>
                <prior>
                    <class>1</class>
                    <probability>0.4</probability>
                </prior>
            </priors>
            <store_source>1</store_source>
            <start_token>&lt;s&gt;</start_token>
            <end_token>&lt;/s&gt;</end_token>
        </naive_bayes>
    </layout>
    <lifetime>3600</lifetime>
</dictionary>
```

</Tab>
</Tabs>

## Tokenization modes {#tokenization-modes}

`mode` decides what a "token" is, and therefore what the n-grams look like. The source n-grams must have been produced with the **same** `mode` and `n`.

- `byte` — each token is a single byte; no UTF-8 assumption. With `n = 2`, `'abc'` yields the byte bigrams `'ab'`, `'bc'`. *Good for* language or encoding detection on arbitrary byte sequences, and any data where sub-character signal matters. Usually paired with `n >= 2`.
- `codepoint` — each token is one Unicode code point; the input is interpreted as UTF-8. With `n = 1`, `'café'` yields the code points `'c'`, `'a'`, `'f'`, `'é'`. *Good for* script and language detection, and short or CJK text where whitespace word boundaries are unreliable. (Source n-grams must be valid UTF-8; query input is decoded leniently — see [Notes](#notes).)
- `token` — each token is a word delimited by **ASCII whitespace** (space, tab, newline, carriage return, form feed, vertical tab; runs collapse to one separator). Non-ASCII Unicode whitespace such as `U+00A0` (no-break space) or `U+2003` (em space) is **not** a separator and stays inside a token. Whitespace is the only thing that splits — nothing is lowercased or stripped — so `'Hello, World!'` becomes the tokens `'Hello,'` and `'World!'` (the comma, the `!`, and the capitals are all kept), and with `n = 2` they form the single bigram `'Hello, World!'`. *Good for* word-level classification on space-separated languages — sentiment, topic, spam, language of a sentence.

## Prior modes {#prior-modes}

The prior is the model's belief about each class *before* it looks at the text. `priors_mode` chooses how it is set.

- `proportional` (default) — each class's prior is proportional to its total n-gram count in the training data — the sum of the `count` column for that class, not its number of rows or training documents — so classes seen more often start out more likely. **Choose it** when the training class proportions (by total n-gram count) match the frequencies you expect at query time. **Nothing to supply** — it is derived from the source counts.

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'proportional'))
  ```

- `uniform` — every class is equally likely to begin with, so no class gets a head start and the prediction comes entirely from the input's n-grams. **Choose it** when the classes are balanced, or when the training frequencies do not reflect how often each class appears at query time. **Nothing to supply.**

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'uniform'))
  ```

- `explicit` — you provide the priors with `priors [(0, 0.6), (1, 0.4)]`: one `(class, probability)` pair per class, each probability greater than 0 and at most 1, together summing to `1.0`. **Choose it** when you know the real base rates and they differ from training — e.g. only 1% of production traffic is spam even though the training set was balanced. **Compute them** from the expected real-world share of each class.

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.9), (1, 0.1)]))
  ```

## Boundary tokens (padding) {#boundary-tokens-padding}

Padding is off by default. It only matters for `n > 1`, where it can improve accuracy by letting the model use signals at the start and end of the text.

**Why it helps.** With `n > 1`, n-grams in the middle of the text get full left and right context, but the first and last tokens do not. Adding boundary tokens creates n-grams that mark "start of text" and "end of text", so the model can learn patterns tied to position — for example a word that is distinctive when it *begins* a message, or a character typical at a word's *end*.

**What you must do:**

1. **Decide per side.** `start_token` and `end_token` are independent — set one, both, or neither. An empty value means that side is not padded.
2. **Choose rare values** that will not collide with real data, e.g. `0x01` / `0xFF` for `byte`, `U+10FFFE` / `U+10FFFF` for `codepoint`, or `<s>` / `</s>` for `token`.
3. **Produce the training n-grams with the same padding.** The dictionary pads the query input but never your source, so the boundary tokens must already be baked into the n-grams you load. The easiest way to guarantee they match is to build the source with [`naiveBayesNgrams`](/reference/functions/regular-functions/splitting-merging-functions#naiveBayesNgrams), passing it the same `start_token` and `end_token` (and `n` and `mode`) you give the layout — it emits exactly the padded n-grams the dictionary produces at query time.

The padding-token format depends on the mode:

- `byte` — a number for the byte value, in decimal or `0x` hex (so `'1'` and `'0x01'` are the same):

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '0x01' end_token '0xFF'))
  ```

- `codepoint` — a number for the UTF-8 code point, in decimal or `0x` hex (so `'1114110'` and `'0x10FFFE'` are the same):

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint' start_token '0x10FFFE' end_token '0x10FFFF'))
  ```

- `token` — the literal token string:

  ```sql
  LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token' start_token '<s>' end_token '</s>'))
  ```

## Build training data from raw text {#build-training-data-from-raw-text}

If you start from raw labelled text instead of pre-aggregated counts, use the [`naiveBayesNgrams`](/reference/functions/regular-functions/splitting-merging-functions#naiveBayesNgrams) function to split it into n-grams. Give it the same `n`, `mode`, `start_token`, and `end_token` as your layout, and it produces exactly the n-grams the dictionary expects, so the training data matches what the model sees at query time.

Given a table of `(class_id, text)` rows, build the `(ngram, class_id, count)` source with one `GROUP BY`:

```sql
CREATE TABLE docs (class_id UInt32, text String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO docs VALUES
    (1, 'The food was amazing and the service was great'),
    (0, 'The service was terrible and the food was awful'),
    (1, 'I loved this cozy little place and the friendly staff'),
    (0, 'I hated the bad weather and the long wait'),
    (1, 'Best dinner we have had here, everything was delicious');

CREATE TABLE training_data (ngram String, class_id UInt32, count UInt64)
ENGINE = MergeTree ORDER BY (class_id, ngram);

INSERT INTO training_data
SELECT ngram, class_id, count()
FROM docs
ARRAY JOIN naiveBayesNgrams(text, 1, 'token') AS ngram
GROUP BY ngram, class_id;
```

```sql
SELECT * FROM training_data ORDER BY ngram LIMIT 5;
```

```response
   ┌─ngram─┬─class_id─┬─count─┐
1. │ Best  │        1 │     1 │
2. │ I     │        1 │     1 │
3. │ I     │        0 │     1 │
4. │ The   │        0 │     1 │
5. │ The   │        1 │     1 │
   └───────┴──────────┴───────┘
```

`training_data` is now a valid source for a `NAIVE_BAYES` dictionary (here token unigrams; change the `n` and `mode` arguments to match your layout). The dictionary tokenizes query input exactly as it is given, so if the training text is lowercased but the query text is not, their n-grams will not match and model accuracy will suffer.

<Info>
**Priors and document counts**

The `proportional` prior (the default) is weighted by each class's **total n-gram count**, not by its number of documents. If you want the classic document-frequency prior (`documents_in_class / total_documents`), compute it from the raw `docs` table and pass it with `priors_mode 'explicit'`:

```sql
SELECT groupArray((class_id, frac)) AS priors
FROM (SELECT class_id, count() / sum(count()) OVER () AS frac FROM docs GROUP BY class_id);
```

```response
   ┌─priors────────────┐
1. │ [(0,0.4),(1,0.6)] │
   └───────────────────┘
```
</Info>

Then, create the dictionary from `training_data`, passing the explicit prior computed above, and classify new reviews:

```sql
CREATE DICTIONARY review_sentiment (ngram String, class_id UInt32, count UInt64)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'training_data'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' priors_mode 'explicit' priors [(0, 0.4), (1, 0.6)]))
LIFETIME(0);
```

```sql
SELECT
    naiveBayesClassifier('review_sentiment', 'amazing food and friendly staff') AS positive_review,
    naiveBayesClassifier('review_sentiment', 'awful service and a terrible meal') AS negative_review;
```

```response
   ┌─positive_review─┬─negative_review─┐
1. │               1 │               0 │
   └─────────────────┴─────────────────┘
```

Class `1` is positive and `0` is negative, so both reviews are classified correctly.

## More examples {#more-examples}

**Byte mode** — byte bigrams (`n = 2`, `mode 'byte'`; class `0` = strings of letters `a`–`d`, class `1` = letters `x`–`z`):

```sql
CREATE TABLE byte_patterns_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO byte_patterns_src VALUES (0,'ab',5),(0,'bc',5),(0,'cd',5),(1,'xy',5),(1,'yz',5),(1,'zw',5);

CREATE DICTIONARY byte_patterns (ngram String, class_id UInt32, count UInt64)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'byte_patterns_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte')) LIFETIME(0);

SELECT naiveBayesClassifier('byte_patterns', 'abcd') AS abcd, naiveBayesClassifier('byte_patterns', 'xyzw') AS xyzw;
```

```response
   ┌─abcd─┬─xyzw─┐
1. │    0 │    1 │
   └──────┴──────┘
```

**Code-point mode** — per-character script detection (`n = 1`, `mode 'codepoint'`; class `0` = Latin, `1` = Cyrillic):

```sql
CREATE TABLE script_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO script_src VALUES (0,'a',5),(0,'b',5),(0,'c',5),(0,'d',5),(1,'а',5),(1,'б',5),(1,'в',5),(1,'г',5);

CREATE DICTIONARY script (ngram String, class_id UInt32, count UInt64)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'script_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint')) LIFETIME(0);

SELECT naiveBayesClassifier('script', 'abcd') AS latin, naiveBayesClassifier('script', 'абвг') AS cyrillic;
```

```response
   ┌─latin─┬─cyrillic─┐
1. │     0 │        1 │
   └───────┴──────────┘
```

**Read the training data back** with `store_source`:

```sql
CREATE TABLE stored_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO stored_src VALUES (0,'alpha',3),(0,'beta',2),(1,'gamma',4);

CREATE DICTIONARY stored (ngram String, class_id UInt32, count UInt64)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'stored_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1)) LIFETIME(0);

SELECT ngram, class_id, count FROM stored ORDER BY ngram;
```

```response
   ┌─ngram─┬─class_id─┬─count─┐
1. │ alpha │        0 │     3 │
2. │ beta  │        0 │     2 │
3. │ gamma │        1 │     4 │
   └───────┴──────────┴───────┘
```

**Language detection from raw text** — short words with boundary padding (`n = 2`, `mode 'codepoint'`; class `0` = English, `1` = Spanish). The training n-grams are built from raw words with [`naiveBayesNgrams`](/reference/functions/regular-functions/splitting-merging-functions#naiveBayesNgrams), and the boundary tokens — passed to both the function and the layout — let the model use the first and last letters of each word:

```sql
CREATE TABLE words (class_id UInt32, text String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO words VALUES
    (0,'dog'),(0,'cat'),(0,'fish'),(0,'bird'),(0,'book'),(0,'hand'),(0,'tree'),(0,'milk'),(0,'duck'),(0,'frog'),(0,'lamp'),(0,'desk'),
    (1,'gato'),(1,'casa'),(1,'perro'),(1,'libro'),(1,'mano'),(1,'leche'),(1,'arbol'),(1,'agua'),(1,'queso'),(1,'fuego'),(1,'mesa'),(1,'silla');

CREATE TABLE word_ngrams (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO word_ngrams
SELECT ngram, class_id, count()
FROM words
ARRAY JOIN naiveBayesNgrams(text, 2, 'codepoint', '0x10FFFE', '0x10FFFF') AS ngram
GROUP BY ngram, class_id;

CREATE DICTIONARY lang (ngram String, class_id UInt32, count UInt64)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'word_ngrams'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint' start_token '0x10FFFE' end_token '0x10FFFF')) LIFETIME(0);

SELECT naiveBayesClassifier('lang', 'window') AS window, naiveBayesClassifier('lang', 'fiesta') AS fiesta;
```

```response
   ┌─window─┬─fiesta─┐
1. │      0 │      1 │
   └────────┴────────┘
```

## Notes {#notes}

- **Computational dictionary semantics.** This is a *computational* dictionary: `dictGet(dict, '<class_attribute>', text)` classifies `text` (the key is an input to classify, not a stored key), the count attribute is not queryable, and `dictHas` always returns `1`.
- **Source validation at load.** Every source n-gram must match the configured `n` and `mode` (in `codepoint` mode it must also be valid UTF-8); a mismatch fails the load. Because zero-count rows are ignored (see [How it works](#how-it-works)), a source that is empty or has only zero counts has nothing to train on and fails to load.
- **Query-time tokenization is lenient.** Unlike source validation, query input is never rejected. In `codepoint` mode, bytes that are not valid UTF-8 are decoded on a best-effort basis instead of failing the query; in `token` mode, only ASCII whitespace separates words (Unicode whitespace such as `U+00A0` stays inside a token). Malformed input still classifies — typically from the priors, since its n-grams will not match the trained ones.
)DOCS_MD",
            .syntax = "LAYOUT(NAIVE_BAYES(class_attribute 'name' n N mode 'byte'|'codepoint'|'token' [alpha 1.0] "
                      "[priors_mode 'proportional'|'uniform'|'explicit'] [priors [(0, 0.6), (1, 0.4)]] [start_token ...] "
                      "[end_token ...] [store_source 0]))",
            .introduced_in = {26, 7}});
}

}
