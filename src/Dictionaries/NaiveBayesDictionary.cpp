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
            .description = "A computational dictionary that trains an immutable Naive Bayes text classifier at load time from "
                           "pre-aggregated per-class n-gram counts, then classifies text through `dictGet` or the "
                           "`naiveBayesClassifier` functions.",
            .syntax = "LAYOUT(NAIVE_BAYES(class_attribute 'name' n N mode 'byte'|'codepoint'|'token' [alpha 1.0] "
                      "[priors_mode 'proportional'|'uniform'|'explicit'] [priors [(0, 0.6), (1, 0.4)]] [start_token ...] "
                      "[end_token ...] [store_source 0]))",
            .introduced_in = {26, 7}});
}

}
