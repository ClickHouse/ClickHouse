#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexLegacyHypothesis.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/escapeForFileName.h>
#include <Common/SipHash.h>

#include <numeric>
#include <typeindex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

bool indexFileExistsInChecksums(
    const MergeTreeDataPartChecksums & checksums,
    const std::string & path_prefix,
    const std::string & extension,
    const IDataPartStorage * storage)
{
    if (checksums.files.contains(path_prefix + extension))
        return true;

    /// Also check for hashed version of the filename
    auto hash = sipHash128String(path_prefix);
    if (checksums.files.contains(hash + extension))
        return true;

    /// Packed substreams: not listed in checksums.txt as individual entries, but the
    /// storage overlay reports their existence via the skp_idx.packed index.
    if (storage && checksums.files.contains(String(SKIP_INDICES_PACKED_FILENAME)))
    {
        if (storage->existsFile(path_prefix + extension))
            return true;
        if (storage->existsFile(hash + extension))
            return true;
    }

    return false;
}

String getIndexFileName(const String & index_name, bool escape_filename)
{
    if (escape_filename)
        return escapeForFileName(String(SKIP_INDEX_FILE_PREFIX) + index_name);
    return String(SKIP_INDEX_FILE_PREFIX) + index_name;
}

String IMergeTreeIndex::getFileName() const
{
    return getIndexFileName(index.name, index.escape_filenames);
}

Names IMergeTreeIndex::getColumnsRequiredForIndexCalc() const
{
    return index.expression->getRequiredColumns();
}

const NamesAndTypesList & IMergeTreeIndex::getColumnsWithTypesRequiredForIndexCalc() const
{
    return index.expression->getRequiredColumnsWithTypes();
}

namespace
{

/// Is a granule written with @from decoded identically when read back as @to?
///
/// This deliberately duplicates part of AlterCommands' isMetadataOnlyConversion() instead of calling
/// it. That predicate answers "does ALTER have to rewrite the data?", which is a weaker question: a
/// JSON typed-path hint change is metadata-only there because column reads convert lazily, but a skip
/// index granule has no such lazy path and decodes the old bytes with the new type. The two must stay
/// free to diverge.
///
/// Fail-closed: anything not listed here is treated as incompatible.
bool isRepresentationPreservingConversion(const IDataType * from, const IDataType * to)
{
    while (true)
    {
        if (from->equals(*to))
            return true;

        /// Extending an enum's value set leaves the bytes alone. Shrinking it does not: a value on
        /// disk may no longer name a valid element, so the allowance is one-directional.
        if (const auto * from_enum8 = typeid_cast<const DataTypeEnum8 *>(from))
        {
            if (const auto * to_enum8 = typeid_cast<const DataTypeEnum8 *>(to))
                return to_enum8->contains(*from_enum8);
        }

        if (const auto * from_enum16 = typeid_cast<const DataTypeEnum16 *>(from))
        {
            if (const auto * to_enum16 = typeid_cast<const DataTypeEnum16 *>(to))
                return to_enum16->contains(*from_enum16);
        }

        /// Width-preserving pairs, oriented part-side -> metadata-side. Int8 -> Enum8 is absent on
        /// purpose: the part may hold an integer that names no element of the new enum, and keeping
        /// the granule then prunes away rows the unindexed read rejects outright.
        static const std::unordered_multimap<std::type_index, const std::type_info &> allowed_conversions =
        {
            { typeid(DataTypeEnum8),    typeid(DataTypeInt8)     },
            { typeid(DataTypeEnum16),   typeid(DataTypeInt16)    },
            { typeid(DataTypeDateTime), typeid(DataTypeUInt32)   },
            { typeid(DataTypeUInt32),   typeid(DataTypeDateTime) },
            { typeid(DataTypeDate),     typeid(DataTypeUInt16)   },
            { typeid(DataTypeUInt16),   typeid(DataTypeDate)     },
        };

        auto it_range = allowed_conversions.equal_range(typeid(*from));
        for (auto it = it_range.first; it != it_range.second; ++it)
        {
            if (it->second == typeid(*to))
                return true;
        }

        const auto * arr_from = typeid_cast<const DataTypeArray *>(from);
        const auto * arr_to = typeid_cast<const DataTypeArray *>(to);
        if (arr_from && arr_to)
        {
            from = arr_from->getNestedType().get();
            to = arr_to->getNestedType().get();
            continue;
        }

        const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from);
        const auto * nullable_to = typeid_cast<const DataTypeNullable *>(to);
        if (nullable_from && nullable_to)
        {
            from = nullable_from->getNestedType().get();
            to = nullable_to->getNestedType().get();
            continue;
        }

        return false;
    }
}

/// Timezone attribution of a DateTime/DateTime64: nullopt for any other type, an empty string when
/// the type carries no timezone of its own, the zone name when it does.
std::optional<String> tryGetTimezoneAttribution(const IDataType * type)
{
    if (const auto * date_time = typeid_cast<const DataTypeDateTime *>(type))
        return date_time->hasExplicitTimeZone() ? date_time->getTimeZone().getTimeZone() : String{};

    if (const auto * date_time64 = typeid_cast<const DataTypeDateTime64 *>(type))
        return date_time64->hasExplicitTimeZone() ? date_time64->getTimeZone().getTimeZone() : String{};

    return {};
}

/// Is a DateTime or DateTime64 reachable from @type at all?
bool containsTimezoneAttribution(const IDataType & type)
{
    if (tryGetTimezoneAttribution(&type))
        return true;

    bool found = false;
    type.forEachChild([&](const IDataType & child)
    {
        if (tryGetTimezoneAttribution(&child))
            found = true;
    });
    return found;
}

/// Do two types that IDataType::equals() reports as equal still attribute their values to different
/// timezones? DataTypeDateTime and DataTypeDateTime64 ignore the timezone in equals() on purpose, and
/// they are the only types under src/DataTypes that drop a semantically load-bearing parameter: every
/// other implementation either compares its parameters or recurses into its children.
///
/// Fail-closed: a shape this cannot compare is reported as a difference, but only where a timezone is
/// reachable at all, so a type that carries none is never refused.
bool hasTimezoneDependentDifference(const IDataType * from, const IDataType * to)
{
    auto from_time_zone = tryGetTimezoneAttribution(from);
    auto to_time_zone = tryGetTimezoneAttribution(to);
    if (from_time_zone || to_time_zone)
        return from_time_zone != to_time_zone;

    /// equals() recurses through these, so their children line up pairwise.
    if (const auto * from_nullable = typeid_cast<const DataTypeNullable *>(from))
    {
        const auto * to_nullable = typeid_cast<const DataTypeNullable *>(to);
        return !to_nullable
            || hasTimezoneDependentDifference(from_nullable->getNestedType().get(), to_nullable->getNestedType().get());
    }

    if (const auto * from_array = typeid_cast<const DataTypeArray *>(from))
    {
        const auto * to_array = typeid_cast<const DataTypeArray *>(to);
        return !to_array
            || hasTimezoneDependentDifference(from_array->getNestedType().get(), to_array->getNestedType().get());
    }

    if (const auto * from_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(from))
    {
        const auto * to_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(to);
        return !to_low_cardinality
            || hasTimezoneDependentDifference(
                from_low_cardinality->getDictionaryType().get(), to_low_cardinality->getDictionaryType().get());
    }

    if (const auto * from_map = typeid_cast<const DataTypeMap *>(from))
    {
        const auto * to_map = typeid_cast<const DataTypeMap *>(to);
        return !to_map
            || hasTimezoneDependentDifference(from_map->getKeyType().get(), to_map->getKeyType().get())
            || hasTimezoneDependentDifference(from_map->getValueType().get(), to_map->getValueType().get());
    }

    if (const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from))
    {
        const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to);
        if (!to_tuple || from_tuple->getElements().size() != to_tuple->getElements().size())
            return true;

        for (size_t i = 0; i < from_tuple->getElements().size(); ++i)
            if (hasTimezoneDependentDifference(from_tuple->getElements()[i].get(), to_tuple->getElements()[i].get()))
                return true;

        return false;
    }

    if (const auto * from_variant = typeid_cast<const DataTypeVariant *>(from))
    {
        const auto * to_variant = typeid_cast<const DataTypeVariant *>(to);
        if (!to_variant || from_variant->getVariants().size() != to_variant->getVariants().size())
            return true;

        for (size_t i = 0; i < from_variant->getVariants().size(); ++i)
            if (hasTimezoneDependentDifference(from_variant->getVariants()[i].get(), to_variant->getVariants()[i].get()))
                return true;

        return false;
    }

    return containsTimezoneAttribution(*from) || containsTimezoneAttribution(*to);
}

}

bool IMergeTreeIndex::isPartTypeCompatible(const IMergeTreeDataPart & part) const
{
    for (const auto & [column, metadata_type] : getColumnsWithTypesRequiredForIndexCalc())
    {
        auto part_column = part.tryGetColumn(column);

        /// The column is in the metadata but not in this part's column list, so there is no
        /// part-side type to compare against. That does NOT mean the part holds no granule: a part
        /// can carry index files for an index whose required column was never written to it (see
        /// MutateTask.cpp, issue #104872). Refuse rather than guess. Callers ask this only about a
        /// part that already has the index on disk, so a part that merely predates ADD INDEX is
        /// unaffected.
        if (!part_column)
            return false;

        /// equals() calls two DateTime types with different timezones equal, so a timezone-only
        /// MODIFY COLUMN reaches here as "no difference" even though an index expression that reads
        /// the timezone (toHour, toStartOfDay, ...) now yields different values than the granule holds.
        if (part_column->type->equals(*metadata_type)
            && !hasTimezoneDependentDifference(part_column->type.get(), metadata_type.get()))
            continue;

        /// A granule of an expression index stores the EXPRESSION's result type, which can change
        /// even when every column conversion is representation-preserving: `d + 1` yields Date for a
        /// Date column but UInt32 once that column becomes UInt16. Recovering the expression's
        /// part-side result type would mean running the analyzer per part on the query path, so a
        /// non-trivial expression is refused on any type difference.
        if (!index.isSimpleSingleColumnIndex())
            return false;

        if (!isRepresentationPreservingConversion(part_column->type.get(), metadata_type.get()))
            return false;
    }

    return true;
}

MergeTreeIndexFormat IMergeTreeIndex::getDeserializedFormat(const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const
{
    for (const auto & [column, _] : getColumnsWithTypesRequiredForIndexCalc())
        if (part.isSystemColumnInvalidated(column))
            return {0 /*unknown*/, {}};

    /// Discover what is on disk first: a part that does not have this index at all cannot
    /// mis-decode anything, and asking the type question only about parts that DO have it lets the
    /// check refuse outright when the part records no type for a required column.
    auto format = getPhysicalFormat(part, relative_path_prefix);
    if (!format)
        return format;

    /// The part's granules were written with types the metadata no longer declares, so decoding them
    /// under the new types reads garbage. Report the index as not materialized in this part; the
    /// query then answers correctly without it.
    if (!isPartTypeCompatible(part))
        return {0 /*unknown*/, {}};

    return format;
}

MergeTreeIndexFormat IMergeTreeIndex::getPhysicalFormat(const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(part.checksums, relative_path_prefix, ".idx", &part.getDataPartStorage()))
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
    StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & settings) const
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

    return it->second(std::move(metadata_snapshot), index, settings);
}

MergeTreeIndices MergeTreeIndexFactory::getMany(StorageMetadataPtr metadata_snapshot, const std::vector<IndexDescription> & indices, const MergeTreeSettings & settings) const
{
    MergeTreeIndices result;
    for (const auto & index : indices)
        result.emplace_back(get(metadata_snapshot, index, settings));
    return result;
}

void MergeTreeIndexFactory::validate(const IndexDescription & index, bool attach, const MergeTreeSettings & settings) const
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

    it->second(index, attach, settings);
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
