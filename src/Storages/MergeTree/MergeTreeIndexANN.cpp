#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/MergeTreeIndexANN.h>

#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/IndicesDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <xxhash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Argument-name constants. Keep them in one place so the same string is used by the parser and
/// the validator (any typo would disable the corresponding option silently otherwise).
const String ARG_ALGORITHM = "algorithm";
const String ARG_METRIC = "metric";
const String ARG_DIM = "dim";
const String ARG_MAX_DEGREE = "max_degree";
const String ARG_BUILD_SEARCH_LIST_SIZE = "build_search_list_size";
const String ARG_ALPHA = "alpha";
const String ARG_PQ_CHUNKS = "pq_chunks";
const String ARG_SEARCH_LIST_SIZE = "search_list_size";
const String ARG_BEAM_WIDTH = "beam_width";
const String ARG_SEARCH_IO_LIMIT = "search_io_limit";
const String ARG_NUM_THREADS = "num_threads";
const String ARG_BUILD_RAM_LIMIT_GB = "build_ram_limit_gb";
const String ARG_HASH_SEED = "hash_seed";

/// Hard upper bound on vector dimensionality. Matches the constraint currently enforced by the
/// DiskANN FFI layer; keep in sync if that ever changes.
constexpr UInt64 MAX_SUPPORTED_DIMENSION = 16384;

/// Only algorithm currently supported. The set is validated as a whitelist so that adding e.g.
/// `hnsw` in the future is a single-line change.
const std::unordered_set<String> SUPPORTED_ALGORITHMS = {"diskann"};

const std::unordered_map<String, UInt8> METRIC_TO_ID = {
    {"L2", 0},
    {"Cosine", 1},
};

/// Parse a `key = value` pair from an `ASTFunction(name="equals", args=[ident, literal])` node.
std::pair<String, ASTPtr> parseNamedArgument(const ASTFunction * ast_equal_function)
{
    if (!ast_equal_function
        || ast_equal_function->name != "equals"
        || ast_equal_function->arguments->children.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN index arguments must be written as `key = value` pairs");

    const auto & arguments = ast_equal_function->arguments;
    const auto * key_identifier = arguments->children[0]->as<ASTIdentifier>();

    if (!key_identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN index argument must be a key-value pair, got: {}", ast_equal_function->formatForErrorMessage());

    return {key_identifier->name(), arguments->children[1]};
}

/// Collect all named arguments into a map. Detects duplicates; rejects positional arguments.
std::unordered_map<String, ASTPtr> convertArgumentsToOptionsMap(const ASTPtr & arguments)
{
    std::unordered_map<String, ASTPtr> options;
    if (!arguments)
        return options;

    for (const auto & child : arguments->children)
    {
        const auto * ast_equal_function = child->as<ASTFunction>();
        auto [key, ast] = parseNamedArgument(ast_equal_function);

        if (!options.emplace(key, ast).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ANN index argument '{}' is specified more than once", key);
    }
    return options;
}

Field literalFromAST(const ASTPtr & ast, std::string_view argument_name)
{
    if (const auto * ast_literal = ast->as<ASTLiteral>())
        return ast_literal->value;
    if (const auto * ast_identifier = ast->as<ASTIdentifier>())
        return Field(ast_identifier->name());
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "ANN index argument '{}' must be a literal or identifier", argument_name);
}

template <typename T>
std::optional<T> extractFieldOption(std::unordered_map<String, ASTPtr> & options, const String & option)
{
    auto it = options.find(option);
    if (it == options.end())
        return {};

    Field value = literalFromAST(it->second, option);

    /// Widen integer literals to the target type. `ASTLiteral` of a non-negative integer is parsed
    /// as `UInt64`, but the user may legitimately write `alpha = 1` and expect it to coerce to
    /// Float32 / Float64.
    auto expected_type = Field::TypeToEnum<T>::value;
    if (value.getType() != expected_type)
    {
        if constexpr (std::is_same_v<T, Float64> || std::is_same_v<T, Float32>)
        {
            if (value.getType() == Field::Types::UInt64)
                value = static_cast<Float64>(value.safeGet<UInt64>());
            else if (value.getType() == Field::Types::Int64)
                value = static_cast<Float64>(value.safeGet<Int64>());
        }
    }

    if (value.getType() != expected_type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN index argument '{}' expected to be {}, but got {}",
            option, fieldTypeToString(expected_type), value.getTypeName());

    options.erase(it);
    return value.safeGet<T>();
}

/// Compute a stable fingerprint of the algorithm tuning parameters so that the `ANNIndexManager`
/// can detect shape changes at startup. The key ordering is sorted to make the hash
/// deterministic regardless of argument order in the DDL.
UInt64 computeParamsHash(
    UInt64 max_degree,
    UInt64 build_search_list_size,
    double alpha,
    UInt64 pq_chunks)
{
    /// Stringify with fixed formatting so that minor representation differences (e.g. `1.2` vs
    /// `1.200000`) hash identically.
    String s = fmt::format(
        "alpha={:.6f}&build_search_list_size={}&max_degree={}&pq_chunks={}",
        alpha,
        build_search_list_size,
        max_degree,
        pq_chunks);
    return XXH64(s.data(), s.size(), /*seed*/ 0);
}

/// Parse options for both creator and validator. Returns the fully populated definition.
/// Throws on any invalid / unexpected argument so that both code paths share validation logic.
ANNIndexDefinition parseANNOptions(const IndexDescription & index)
{
    auto options = convertArgumentsToOptionsMap(index.arguments);

    String algorithm = extractFieldOption<String>(options, ARG_ALGORITHM).value_or("diskann");
    if (!SUPPORTED_ALGORITHMS.contains(algorithm))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported ANN algorithm '{}'; supported: {}",
            algorithm, fmt::join(SUPPORTED_ALGORITHMS, ", "));

    String metric = extractFieldOption<String>(options, ARG_METRIC).value_or("L2");
    auto metric_it = METRIC_TO_ID.find(metric);
    if (metric_it == METRIC_TO_ID.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported ANN metric '{}'; supported: L2, Cosine", metric);

    auto dim_opt = extractFieldOption<UInt64>(options, ARG_DIM);
    if (!dim_opt.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN index requires the 'dim' argument (vector dimensionality)");
    UInt64 dim = *dim_opt;
    if (dim == 0 || dim > MAX_SUPPORTED_DIMENSION)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN index argument 'dim' must be in [1, {}], got {}", MAX_SUPPORTED_DIMENSION, dim);

    UInt64 max_degree = extractFieldOption<UInt64>(options, ARG_MAX_DEGREE).value_or(64);
    UInt64 build_search_list_size = extractFieldOption<UInt64>(options, ARG_BUILD_SEARCH_LIST_SIZE).value_or(100);
    Float64 alpha = extractFieldOption<Float64>(options, ARG_ALPHA).value_or(1.2);
    UInt64 pq_chunks = extractFieldOption<UInt64>(options, ARG_PQ_CHUNKS).value_or(0);

    UInt64 search_list_size = extractFieldOption<UInt64>(options, ARG_SEARCH_LIST_SIZE).value_or(100);
    UInt64 beam_width = extractFieldOption<UInt64>(options, ARG_BEAM_WIDTH).value_or(4);
    UInt64 search_io_limit = extractFieldOption<UInt64>(options, ARG_SEARCH_IO_LIMIT).value_or(4);
    UInt64 num_threads = extractFieldOption<UInt64>(options, ARG_NUM_THREADS).value_or(1);
    Float64 build_ram_limit_gb = extractFieldOption<Float64>(options, ARG_BUILD_RAM_LIMIT_GB).value_or(0.0);

    UInt64 hash_seed = extractFieldOption<UInt64>(options, ARG_HASH_SEED).value_or(0);

    if (!options.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unexpected ANN index arguments: {}", fmt::join(std::views::keys(options), ", "));

    /// The validator can be called before `expression` is materialised; `column_names` is always
    /// populated by `IndexDescription::getIndexFromAST` / test setup.
    String vector_column_name;
    if (!index.column_names.empty())
        vector_column_name = index.column_names[0];

    ANNIndexDefinition definition;
    definition.shape.dim = static_cast<UInt32>(dim);
    definition.shape.metric = metric_it->second;
    definition.shape.algorithm = algorithm;
    definition.shape.params_hash = computeParamsHash(max_degree, build_search_list_size, alpha, pq_chunks);

    definition.build_options.max_degree = static_cast<uint32_t>(max_degree);
    definition.build_options.l_build = static_cast<uint32_t>(build_search_list_size);
    definition.build_options.alpha = static_cast<float>(alpha);
    definition.build_options.num_threads = static_cast<uint32_t>(num_threads);
    if (build_ram_limit_gb > 0.0)
        definition.build_options.build_ram_limit_gb = build_ram_limit_gb;
    if (pq_chunks > 0)
        definition.build_options.pq_chunks = static_cast<uint32_t>(pq_chunks);

    DiskANNSearchOptions disk_search_opts;
    disk_search_opts.default_search_list_size = static_cast<uint32_t>(search_list_size);
    disk_search_opts.default_beam_width = static_cast<uint32_t>(beam_width);
    disk_search_opts.search_io_limit = static_cast<uint32_t>(search_io_limit);
    definition.search_defaults = std::make_shared<DiskANNSearchDefaults>(disk_search_opts);

    definition.hash_seed = hash_seed;
    definition.vector_column_name = vector_column_name;

    return definition;
}

/// Resolve the source data type of the index. The sample_block is the ground truth once the
/// expression has been prepared, but for validator invocations that happen before that step we
/// fall back to `index.data_types`.
DataTypePtr resolveIndexDataType(const IndexDescription & index)
{
    if (index.sample_block.columns() > 0)
        return index.sample_block.getDataTypes()[0];
    if (!index.data_types.empty())
        return index.data_types[0];
    return nullptr;
}

void validateColumnType(const IndexDescription & index)
{
    if (index.column_names.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "ANN index must be created on a single column, got {}", index.column_names.size());

    DataTypePtr data_type = resolveIndexDataType(index);
    if (!data_type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ANN index has no resolvable column type");

    const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "ANN index requires a column of type Array(Float32), got: {}", data_type->getName());

    if (array_type->getNestedType()->getTypeId() != TypeIndex::Float32)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "ANN index requires a column of type Array(Float32), got: {}", data_type->getName());
}

}

MergeTreeIndexANN::MergeTreeIndexANN(const IndexDescription & index_, ANNIndexDefinition definition_)
    : IMergeTreeIndex(index_)
    , definition(std::move(definition_))
{
}

MergeTreeIndexPtr annIndexCreator(const IndexDescription & index)
{
    ANNIndexDefinition definition = parseANNOptions(index);
    return std::make_shared<MergeTreeIndexANN>(index, std::move(definition));
}

void annIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    /// Parameter-level validation - exercised on both CREATE and ATTACH.
    (void)parseANNOptions(index);
    validateColumnType(index);
}

bool extractANNShapeFromMetadata(const StorageInMemoryMetadata & metadata, ANNIndexShapeFingerprint & out_shape)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == "ann")
        {
            ANNIndexDefinition definition = parseANNOptions(index);
            out_shape = definition.shape;
            return true;
        }
    }
    return false;
}

bool extractANNDefinitionFromMetadata(const StorageInMemoryMetadata & metadata, ANNIndexDefinition & out_definition)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == "ann")
        {
            out_definition = parseANNOptions(index);
            return true;
        }
    }
    return false;
}

String getANNIndexColumnName(const StorageInMemoryMetadata & metadata)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == "ann" && !index.column_names.empty())
            return index.column_names[0];
    }
    return {};
}

void validateNoCoexistingANNAndVectorSimilarity(const StorageInMemoryMetadata & metadata)
{
    std::unordered_set<String> ann_cols;
    std::unordered_set<String> vs_cols;
    for (const auto & index : metadata.secondary_indices)
    {
        if (!index.expression)
            continue;
        auto required = index.expression->getRequiredColumns();
        if (index.type == "ann")
            for (const auto & c : required)
                ann_cols.insert(c);
        else if (index.type == "vector_similarity")
            for (const auto & c : required)
                vs_cols.insert(c);
    }
    for (const auto & col : ann_cols)
    {
        if (vs_cols.contains(col))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column '{}' cannot have both 'ann' and 'vector_similarity' indexes at the same time", col);
    }
}

}

#endif
