#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexLegacyHypothesis.h>

#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Common/escapeForFileName.h>
#include <Common/SipHash.h>

#include <numeric>

namespace DB
{

constexpr auto INDEX_FILE_PREFIX = "skp_idx_";

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

bool indexFileExistsInChecksums(
    const MergeTreeDataPartChecksums & checksums,
    const std::string & path_prefix,
    const std::string & extension)
{
    if (checksums.files.contains(path_prefix + extension))
        return true;

    /// Also check for hashed version of the filename
    auto hash = sipHash128String(path_prefix);
    return checksums.files.contains(hash + extension);
}

String getIndexFileName(const String & index_name, bool escape_filename)
{
    if (escape_filename)
        return escapeForFileName(INDEX_FILE_PREFIX + index_name);
    return INDEX_FILE_PREFIX + index_name;
}

String IMergeTreeIndex::getFileName() const
{
    return getIndexFileName(index.name, index.escape_filenames);
}

Names IMergeTreeIndex::getColumnsRequiredForIndexCalc() const
{
    return index.expression->getRequiredColumns();
}

MergeTreeIndexFormat IMergeTreeIndex::getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx"))
        return {1, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}};

    return {0 /*unknown*/, {}};
}

void IMergeTreeIndexGranule::serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const
{
    auto * stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    serializeBinary(stream->compressed_hashing);
}

void MergeTreeIndexFactory::registerCreator(const std::string & index_type, Creator creator, Documentation documentation)
{
    if (!creators.emplace(index_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeIndexFactory: the Index creator name '{}' is not unique",
                        index_type);
    documentations.emplace(index_type, std::move(documentation));
}

Documentation MergeTreeIndexFactory::getDocumentation(const std::string & index_type) const
{
    if (auto it = documentations.find(index_type); it != documentations.end())
        return it->second;
    return {};
}
void MergeTreeIndexFactory::registerValidator(const std::string & index_type, Validator validator)
{
    if (!validators.emplace(index_type, std::move(validator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeIndexFactory: the Index validator name '{}' is not unique", index_type);
}

std::vector<String> MergeTreeIndexFactory::getAllRegisteredNames() const
{
    std::vector<String> result;
    result.reserve(creators.size());
    for (const auto & pair : creators)
        result.push_back(pair.first);
    return result;
}

void IMergeTreeIndexGranule::deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    auto * stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    deserializeBinary(*stream->getDataBuffer(), state.version);
}

MergeTreeIndexPtr MergeTreeIndexFactory::get(
    const IndexDescription & index) const
{
    auto it = creators.find(index.type);
    if (it == creators.end())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Unknown Index type '{}'. Available index types: {}", index.type,
                std::accumulate(creators.cbegin(), creators.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            return left + ", " + right.first;
                        })
                );
    }

    return it->second(index);
}


MergeTreeIndices MergeTreeIndexFactory::getMany(const std::vector<IndexDescription> & indices) const
{
    MergeTreeIndices result;
    for (const auto & index : indices)
        result.emplace_back(get(index));
    return result;
}

void MergeTreeIndexFactory::validate(const IndexDescription & index, bool attach) const
{
    /// Do not allow constant and non-deterministic expressions.
    /// Do not throw on attach for compatibility.
    if (!attach)
    {
        if (index.expression->hasArrayJoin())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Secondary index '{}' cannot contain array joins", index.name);

        try
        {
            index.expression->assertDeterministic();
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("for secondary index '{}'", index.name));
            throw;
        }

        for (const auto & elem : index.sample_block)
            if (elem.column && (isColumnConst(*elem.column) || elem.column->isDummy()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Secondary index '{}' cannot contain constants", index.name);
    }

    auto it = validators.find(index.type);
    if (it == validators.end())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Unknown Index type '{}'. Available index types: {}", index.type,
                std::accumulate(
                    validators.cbegin(),
                    validators.cend(),
                    std::string{},
                    [](auto && left, const auto & right) -> std::string
                    {
                        if (left.empty())
                            return right.first;
                        return left + ", " + right.first;
                    })
            );
    }

    it->second(index, attach);
}

MergeTreeIndexFactory::MergeTreeIndexFactory()
{
    registerCreator("minmax", minmaxIndexCreator, Documentation{
        .description = "Stores the minimum and maximum values of the index expression for each granule, allowing granules to be skipped when a query's range condition cannot match.",
        .syntax = "INDEX name expr TYPE minmax GRANULARITY n",
        .related = {"set"}});
    registerValidator("minmax", minmaxIndexValidator);

    registerCreator("set", setIndexCreator, Documentation{
        .description = "Stores up to `max_rows` distinct values of the index expression per granule (0 means unlimited), allowing granules to be skipped for equality and IN conditions.",
        .syntax = "INDEX name expr TYPE set(max_rows) GRANULARITY n",
        .related = {"bloom_filter", "minmax"}});
    registerValidator("set", setIndexValidator);

    registerCreator("ngrambf_v1", bloomFilterIndexTextCreator, Documentation{
        .description = "A Bloom filter over all n-grams of the index expression's string values, for speeding up LIKE, IN, equality and similar searches on substrings.",
        .syntax = "INDEX name expr TYPE ngrambf_v1(n, size_in_bytes, num_hash_functions, seed) GRANULARITY g",
        .related = {"tokenbf_v1", "bloom_filter", "text"}});
    registerValidator("ngrambf_v1", bloomFilterIndexTextValidator);

    registerCreator("tokenbf_v1", bloomFilterIndexTextCreator, Documentation{
        .description = "A Bloom filter over the tokens (words) of the index expression's string values, for speeding up searches of whole tokens.",
        .syntax = "INDEX name expr TYPE tokenbf_v1(size_in_bytes, num_hash_functions, seed) GRANULARITY g",
        .related = {"ngrambf_v1", "text"}});
    registerValidator("tokenbf_v1", bloomFilterIndexTextValidator);

    registerCreator("sparse_grams", bloomFilterIndexTextCreator, Documentation{
        .description = "A Bloom filter over the sparse n-grams of the index expression's string values, for speeding up substring searches.",
        .syntax = "INDEX name expr TYPE sparse_grams(min_ngram_length, max_ngram_length[, min_cutoff_length], size_in_bytes, num_hash_functions, seed) GRANULARITY g",
        .related = {"ngrambf_v1", "tokenbf_v1"}});
    registerValidator("sparse_grams", bloomFilterIndexTextValidator);

    registerCreator("bloom_filter", bloomFilterIndexCreator, Documentation{
        .description = "A Bloom filter over the values of the index expression, for speeding up equality and IN conditions on columns, including Array and Map elements.",
        .syntax = "INDEX name expr TYPE bloom_filter([false_positive_rate]) GRANULARITY g",
        .related = {"set", "tokenbf_v1"}});
    registerValidator("bloom_filter", bloomFilterIndexValidator);

#if USE_USEARCH
    registerCreator("vector_similarity", vectorSimilarityIndexCreator, Documentation{
        .description = "An approximate nearest-neighbour index over a vector column (built using HNSW), for speeding up `ORDER BY <distance_function>(vector, reference) LIMIT n` queries.",
        .syntax = "INDEX name vector TYPE vector_similarity('hnsw', 'distance_function', dimensions[, quantization, hnsw_max_connections_per_layer, hnsw_candidate_list_size_for_construction]) GRANULARITY g",
        .related = {}});
    registerValidator("vector_similarity", vectorSimilarityIndexValidator);
#endif

    registerCreator("text", textIndexCreator, Documentation{
        .description = "A full-text (inverted) index over the tokens of a string column, for speeding up text search functions such as `hasToken`, `hasAnyTokens`, `hasAllTokens`, and `hasPhrase`.",
        .syntax = "INDEX name expr TYPE text(tokenizer = splitByNonAlpha) GRANULARITY g",
        .related = {"tokenbf_v1"}});
    registerValidator("text", textIndexValidator);

    /// Index type 'hypothesis' is no longer supported.
    /// To allow loading tables with old indexes, register a dummy index which allows attach but
    /// throws an exception when the user attempts to create or use it.
    registerCreator("hypothesis", legacyHypothesisIndexCreator, Documentation{
        .description = "Deprecated and no longer supported. It is retained only so that tables which still reference it can be attached.",
        .syntax = "INDEX name expr TYPE hypothesis GRANULARITY g",
        .related = {}});
    registerValidator("hypothesis", legacyHypothesisIndexValidator);
}

MergeTreeIndexFactory & MergeTreeIndexFactory::instance()
{
    static MergeTreeIndexFactory instance;
    return instance;
}

}
