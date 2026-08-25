#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexLegacyHypothesis.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
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
    extern const int BAD_ARGUMENTS;
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

    /// Here the name becomes a part of the file name as is, so a '/' in it would turn into a path
    /// separator. `getIndexFromAST` rejects such names, but `escape_index_filenames` can also be
    /// switched off by `ALTER TABLE ... MODIFY SETTING` after the index was created.
    if (index_name.contains('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Skip index name ({}) cannot contain '/' with `escape_index_filenames` disabled", index_name);

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

        /// Pairwise, like the wrappers above: the framing (a dictionary plus indexes) belongs to the
        /// wrapper, so adding or dropping it is a difference no allow-list entry can cover.
        const auto * low_cardinality_from = typeid_cast<const DataTypeLowCardinality *>(from);
        const auto * low_cardinality_to = typeid_cast<const DataTypeLowCardinality *>(to);
        if (low_cardinality_from && low_cardinality_to)
        {
            from = low_cardinality_from->getDictionaryType().get();
            to = low_cardinality_to->getDictionaryType().get();
            continue;
        }

        return false;
    }
}

/// Do two types that IDataType::equals() reports as equal still mean different things?
///
/// equals() answers "is the on-disk representation the same?", and several implementations
/// deliberately drop an attribute that no byte of the representation depends on but that an index
/// EXPRESSION does read:
///   - DataTypeDateTime / DataTypeDateTime64 ignore the timezone, so toHour(dt) changes while the
///     stored epoch values do not (also reached through a nested AggregateFunction argument, whose
///     equals() compares its argument types with equals() in turn);
///   - a custom name over a plain type (Bool over UInt8, the geo types over tuples) is invisible to
///     the underlying DataTypeNumber<T>::equals(), which compares only typeid, so toString(v)
///     changes from '0' to 'false'.
///
/// getName() is the one projection that carries every such attribute: IDataType::getName() returns
/// the custom name when present and each composite's doGetName() renders its children's names, so
/// this covers wrappers and nested arguments without a branch per type.
///
/// Only valid on the equals-equal path. On the NOT-equals path the difference is a real conversion
/// and isRepresentationPreservingConversion() decides, where a name comparison would wrongly refuse
/// the whole allow-list (Enum8('a'=1) -> Int8 and friends).
bool hasSameMeaning(const IDataType & from, const IDataType & to)
{
    return from.getName() == to.getName();
}

/// The part-side type of a required column, read from the part's OWN column list.
///
/// IMergeTreeDataPart::tryGetColumn() answers from `columns_description`, a storage-wide interning
/// cache (MergeTreeData::getColumnsDescriptionForColumns) whose key equality is
/// NamesAndTypesList::operator== -> IDataType::equals() and whose hash ignores types entirely. Two
/// parts whose column lists differ only by an attribute equals() drops therefore SHARE one entry,
/// and whichever part is loaded first decides what both of them report. Part loading is concurrent
/// and shuffled, so consulting it here would make the comparison below depend on load order.
///
/// A subcolumn (`p.x`, `j.a`) is not a top-level entry, so it is resolved against the part's OWN
/// parent type instead: the carrier of an attribute equals() drops is the parent, and the
/// subcolumn's type is derived from it. @required is the part-side pair, which has already split
/// the name (`a.b.c` is ambiguous between `a` + `b.c` and `a.b` + `c`).
///
/// Returns null when the part's own list answers neither way. For a subcolumn that is a
/// DIFFERENCE, not an unknown, so the caller must not fall back to the cached description there.
template <typename Part>
DataTypePtr tryGetPartOwnType(const Part & part, const NameAndTypePair & required)
{
    if (auto own = part.getColumns().tryGetByName(required.name))
        return own->type;

    if (required.isSubcolumn())
        if (auto parent = part.getColumns().tryGetByName(required.getNameInStorage()))
            return parent->type->tryGetSubcolumnType(required.getSubcolumnName());

    return nullptr;
}

/// Is @subcolumn_name of @name_in_storage a subcolumn this part's own list cannot EXPRESS, for a
/// parent the part does carry? A Quantized codec attaches a custom SERIALIZATION to expose
/// `<col>.quantized` (ColumnsDescription::attachQuantizeSerializationIfNeeded), and columns.txt
/// round-trips only the bare type name (IMergeTreeReader.cpp), so a part's own list cannot describe
/// such a subcolumn at all. Its silence is then an unrepresentable fact, not a type difference.
///
/// All three clauses are load-bearing.
///
/// A custom serialization alone does not mean THIS suffix comes from it: Bool carries one
/// (DataTypeDomainBool.cpp) while defining no subcolumn whatsoever, so without the second clause an
/// unrelated parent would wave through a required column the part genuinely lacks.
/// tryGetSubcolumnType is the right primitive because IDataType builds its SubstreamData from
/// getDefaultSerialization, which returns the custom one when attached.
///
/// The third clause bounds the escape to what it is about. The part being silent about a subcolumn is
/// only unrepresentable while the part HOLDS the parent - a parent the part does not carry at all is
/// ordinary absence, and its granule holds bytes of whatever type the column had when it was written,
/// so it must be refused like any other absent column. Without this clause an index over a
/// serialization-defined subcolumn of a column the part never received would skip the type check
/// entirely and prune with a stale granule.
///
/// A subcolumn of the DECLARED type is unaffected: a Tuple element or a JSON typed path is compared
/// by the parent's equals() as usual, so a stale hint is still caught.
bool hasSerializationDefinedSubcolumns(
    const ColumnsDescription & metadata_columns,
    const NamesAndTypesList & part_columns,
    const String & name_in_storage,
    const String & subcolumn_name)
{
    const auto * parent = metadata_columns.tryGet(name_in_storage);
    return parent && parent->type->getCustomSerialization() != nullptr
        && parent->type->tryGetSubcolumnType(subcolumn_name) != nullptr
        && part_columns.tryGetByName(name_in_storage).has_value();
}

/// The same question for a name that has not been split yet, which also answers "is it a subcolumn
/// at all": a physical column of that exact name is not one, and a name with no separator yields no
/// pair and so no parent. `a.b.c` is ambiguous between `a` + `b.c` and `a.b` + `c`, so try the exact
/// name first and then each split in turn, exactly as Nested::tryGetColumnNameInStorage does - a
/// split whose parent does not define the suffix, or which this part does not carry, must keep
/// looking, since `a` may resolve before `a.b` does.
bool isSerializationDefinedSubcolumn(
    const ColumnsDescription & metadata_columns, const NamesAndTypesList & part_columns, const String & column)
{
    if (metadata_columns.tryGet(column))
        return false;

    for (const auto & [name_in_storage, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(column))
        if (hasSerializationDefinedSubcolumns(
                metadata_columns, part_columns, String(name_in_storage), String(subcolumn_name)))
            return true;

    return false;
}

}

namespace
{

/// The two part representations answer these questions with the same method names, so the checks
/// below are written once. A concrete part must not be wrapped in a part-info here: the physical
/// format is also asked from ~IMergeTreeDataPart, where the part can no longer be shared.
const MergeTreeDataPartChecksums & getChecksums(const IMergeTreeDataPart & part) { return part.checksums; }
const MergeTreeDataPartChecksums & getChecksums(const IMergeTreeDataPartInfoForReader & part) { return part.getChecksums(); }
const IDataPartStorage & getStorage(const IMergeTreeDataPart & part) { return part.getDataPartStorage(); }
const IDataPartStorage & getStorage(const IMergeTreeDataPartInfoForReader & part) { return *part.getDataPartStorage(); }

template <typename Part>
bool isPartTypeCompatibleImpl(const IMergeTreeIndex & skip_index, const Part & part)
{
    const auto & metadata_columns = skip_index.metadata_snapshot->getColumns();
    /// The part's OWN list, for the same reason tryGetPartOwnType uses it rather than the cache.
    const auto & part_columns = part.getColumns();

    for (const auto & [column, metadata_type] : skip_index.getColumnsWithTypesRequiredForIndexCalc())
    {
        auto part_column = part.tryGetColumn(column);

        /// The column is in the metadata but not in this part's column list, so there is no
        /// part-side type to compare against. That does NOT mean the part holds no granule: a part
        /// can carry index files for an index whose required column was never written to it (see
        /// MutateTask.cpp, issue #104872). Refuse rather than guess. Callers ask this only about a
        /// part that already has the index on disk, so a part that merely predates ADD INDEX is
        /// unaffected.
        if (!part_column)
        {
            /// Unless the part's list simply cannot express this subcolumn OF A PARENT THIS PART
            /// CARRIES; see isSerializationDefinedSubcolumn. A genuinely absent column, parent
            /// included, still refuses.
            if (isSerializationDefinedSubcolumn(metadata_columns, part_columns, column))
                continue;
            return false;
        }

        /// Prefer the part's own uncached list; see tryGetPartOwnType() for why the cached one cannot
        /// answer this question. tryGetColumn() above stays the EXISTENCE test: its nullopt is what
        /// drives the refusal right above, and it is what splits a subcolumn's name.
        auto part_type = tryGetPartOwnType(part, *part_column);
        if (!part_type)
        {
            /// The part's own parent type does not offer this subcolumn at all (its element was
            /// renamed, or the parent is absent from this part). Refuse: falling back to the cached
            /// description here would reintroduce the load-order dependency above.
            if (part_column->isSubcolumn())
            {
                /// The same unrepresentable fact reached through the other branch: the interned
                /// description can hand this part a customized parent instance, because no
                /// IDataType::equals() compares custom_serialization, so tryGetColumn() succeeds
                /// while the part's OWN parent still cannot offer the subcolumn. Which branch fires
                /// depends on part load order, so both ask the same question - the parent-presence
                /// clause included.
                if (hasSerializationDefinedSubcolumns(
                        metadata_columns, part_columns, part_column->getNameInStorage(), part_column->getSubcolumnName()))
                    continue;
                return false;
            }
            part_type = part_column->type;
        }

        /// A representation-preserving difference is not necessarily a meaning-preserving one: a
        /// timezone-only MODIFY COLUMN, or UInt8 -> Bool, leaves every byte alone and so reaches here
        /// as "equal", while an index expression that reads the dropped attribute (toHour(dt),
        /// toString(v)) now yields different values than the granule holds.
        if (part_type->equals(*metadata_type) && hasSameMeaning(*part_type, *metadata_type))
            continue;

        /// A granule of an expression index stores the EXPRESSION's result type, which can change
        /// even when every column conversion is representation-preserving: `d + 1` yields Date for a
        /// Date column but UInt32 once that column becomes UInt16. Recovering the expression's
        /// part-side result type would mean running the analyzer per part on the query path, so a
        /// non-trivial expression is refused on any type difference.
        if (!skip_index.index.isSimpleSingleColumnIndex())
            return false;

        if (!isRepresentationPreservingConversion(part_type.get(), metadata_type.get()))
            return false;
    }

    return true;
}

template <typename Part>
MergeTreeIndexFormat getDeserializedFormatImpl(
    const IMergeTreeIndex & skip_index, const Part & part, const std::string & relative_path_prefix)
{
    for (const auto & [column, _] : skip_index.getColumnsWithTypesRequiredForIndexCalc())
        if (part.isSystemColumnInvalidated(column))
            return {0 /*unknown*/, {}};

    /// Discover what is on disk first: a part that does not have this index at all cannot
    /// mis-decode anything, and asking the type question only about parts that DO have it lets the
    /// check refuse outright when the part records no type for a required column.
    auto format = skip_index.getPhysicalFormat(getChecksums(part), getStorage(part), relative_path_prefix);
    if (!format)
        return format;

    /// The part's granules were written with types the metadata no longer declares, so decoding them
    /// under the new types reads garbage. Report the index as not materialized in this part; the
    /// query then answers correctly without it.
    if (!isPartTypeCompatibleImpl(skip_index, part))
        return {0 /*unknown*/, {}};

    return format;
}

}

bool IMergeTreeIndex::isPartTypeCompatible(const IMergeTreeDataPartInfoForReader & part_info) const
{
    return isPartTypeCompatibleImpl(*this, part_info);
}

bool IMergeTreeIndex::isPartTypeCompatible(const IMergeTreeDataPart & part) const
{
    return isPartTypeCompatibleImpl(*this, part);
}

MergeTreeIndexFormat IMergeTreeIndex::getDeserializedFormat(
    const IMergeTreeDataPartInfoForReader & part_info, const std::string & relative_path_prefix) const
{
    return getDeserializedFormatImpl(*this, part_info, relative_path_prefix);
}

MergeTreeIndexFormat IMergeTreeIndex::getDeserializedFormat(const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const
{
    return getDeserializedFormatImpl(*this, part, relative_path_prefix);
}

MergeTreeIndexFormat IMergeTreeIndex::getPhysicalFormat(
    const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx", &storage))
        return {1, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}};

    return {0 /*unknown*/, {}};
}

MergeTreeIndexFormat IMergeTreeIndex::getPhysicalFormat(const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const
{
    return getPhysicalFormat(part.checksums, part.getDataPartStorage(), relative_path_prefix);
}

MergeTreeIndexFormat IMergeTreeIndex::getPhysicalFormat(
    const IMergeTreeDataPartInfoForReader & part_info, const std::string & relative_path_prefix) const
{
    return getPhysicalFormat(part_info.getChecksums(), *part_info.getDataPartStorage(), relative_path_prefix);
}

MergeTreeIndexSubstreams IMergeTreeIndex::getAllSubstreamsInPart(
    const MergeTreeDataPartChecksums & checksums,
    const std::string & relative_path_prefix,
    const IDataPartStorage * storage) const
{
    /// Not routed through `getDeserializedFormat`: that answers the read-time question and
    /// reports nothing once a required system column is invalidated, while a file left on disk
    /// still has to be skipped/stripped here. (minmax overrides to add its legacy `.idx`.)
    MergeTreeIndexSubstreams substreams;
    for (const auto & substream : getSubstreams())
        if (indexFileExistsInChecksums(checksums, relative_path_prefix + substream.suffix, substream.extension, storage))
            substreams.push_back(substream);

    return substreams;
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
