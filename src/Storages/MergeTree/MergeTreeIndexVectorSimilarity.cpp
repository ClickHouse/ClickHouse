#ifdef ENABLE_USEARCH

#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>

#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

namespace ProfileEvents
{
    extern const Event USearchAddCount;
    extern const Event USearchAddVisitedMembers;
    extern const Event USearchAddComputedDistances;
    extern const Event USearchSearchCount;
    extern const Event USearchSearchVisitedMembers;
    extern const Event USearchSearchComputedDistances;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

std::set<String> methods = {"hnsw"};

std::unordered_map<String, unum::usearch::metric_kind_t> nameToMetricKind = {
    {"L2Distance", unum::usearch::metric_kind_t::l2sq_k},
    {"cosineDistance", unum::usearch::metric_kind_t::cos_k}};

std::unordered_map<String, unum::usearch::scalar_kind_t> nameToScalarKind = {
    {"f32", unum::usearch::scalar_kind_t::f32_k},
    {"f16", unum::usearch::scalar_kind_t::f16_k},
    {"i8", unum::usearch::scalar_kind_t::i8_k}};

template<typename T>
concept is_set = std::same_as<T, std::set<typename T::key_type, typename T::key_compare, typename T::allocator_type>>;

template<typename T>
concept is_unordered_map = std::same_as<T, std::unordered_map<typename T::key_type, typename T::mapped_type, typename T::hasher, typename T::key_equal, typename T::allocator_type>>;

template <typename T>
String joinByComma(const T & t)
{
    if constexpr (is_set<T>)
    {
        return fmt::format("{}", fmt::join(t, ", "));
    }
    else if constexpr (is_unordered_map<T>)
    {
        String joined_keys;
        for (const auto & [k, _] : t)
        {
            if (!joined_keys.empty())
                joined_keys += ", ";
            joined_keys += k;
        }
        return joined_keys;
    }
    std::unreachable();
}

}

USearchIndexWithSerialization::USearchIndexWithSerialization(
        size_t dimensions,
        unum::usearch::metric_kind_t metric_kind,
        unum::usearch::scalar_kind_t scalar_kind,
        UsearchHnswParams usearch_hnsw_params)
    : Base(Base::make(unum::usearch::metric_punned_t(dimensions, metric_kind, scalar_kind),
                      unum::usearch::index_dense_config_t(usearch_hnsw_params.m, usearch_hnsw_params.ef_construction, usearch_hnsw_params.ef_search)))
{
}

void USearchIndexWithSerialization::serialize(WriteBuffer & ostr) const
{
    auto callback = [&ostr](void * from, size_t n)
    {
        ostr.write(reinterpret_cast<const char *>(from), n);
        return true;
    };

    Base::save_to_stream(callback);
}

void USearchIndexWithSerialization::deserialize(ReadBuffer & istr)
{
    auto callback = [&istr](void * from, size_t n)
    {
        istr.readStrict(reinterpret_cast<char *>(from), n);
        return true;
    };

    Base::load_from_stream(callback);
}

MergeTreeIndexGranuleVectorSimilarity::MergeTreeIndexGranuleVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , index(nullptr)
{
}

MergeTreeIndexGranuleVectorSimilarity::MergeTreeIndexGranuleVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_,
    USearchIndexWithSerializationPtr index_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
    , index(std::move(index_))
{
}

void MergeTreeIndexGranuleVectorSimilarity::serializeBinary(WriteBuffer & ostr) const
{
    /// Number of dimensions is required in the index constructor,
    /// so it must be written and read separately from the other part
    writeIntBinary(static_cast<UInt64>(index->dimensions()), ostr); // write dimension
    index->serialize(ostr);
}

void MergeTreeIndexGranuleVectorSimilarity::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 dimension;
    readIntBinary(dimension, istr);
    index = std::make_shared<USearchIndexWithSerialization>(dimension, metric_kind, scalar_kind, usearch_hnsw_params);
    index->deserialize(istr);
}

MergeTreeIndexAggregatorVectorSimilarity::MergeTreeIndexAggregatorVectorSimilarity(
    const String & index_name_,
    const Block & index_sample_block_,
    unum::usearch::metric_kind_t metric_kind_,
    unum::usearch::scalar_kind_t scalar_kind_,
    UsearchHnswParams usearch_hnsw_params_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarity::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index_name, index_sample_block, metric_kind, scalar_kind, usearch_hnsw_params, index);
    index = nullptr;
    return granule;
}

void MergeTreeIndexAggregatorVectorSimilarity::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (rows_read == 0)
        return;

    if (rows_read > std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Index granularity is too big: more than 4B rows per index granule.");

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected block with single column");

    const String & index_column_name = index_sample_block.getByPosition(0).name;
    ColumnPtr column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);

    if (const auto & column_array = typeid_cast<const ColumnArray *>(column_cut.get()))
    {
        const auto & column_array_data = column_array->getData();
        const auto & column_array_data_float = typeid_cast<const ColumnFloat32 &>(column_array_data);
        const auto & column_array_data_float_data = column_array_data_float.getData();

        const auto & column_array_offsets = column_array->getOffsets();
        const size_t num_rows = column_array_offsets.size();

        if (column_array->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array is unexpectedly empty");

        /// The Usearch algorithm naturally assumes that the indexed vectors have dimension >= 1. This condition is violated if empty arrays
        /// are INSERTed into an Usearch-indexed column or if no value was specified at all in which case the arrays take on their default
        /// values which is also empty.
        if (column_array->isDefaultAt(0))
            throw Exception(ErrorCodes::INCORRECT_DATA, "The arrays in column '{}' must not be empty. Did you try to INSERT default values?", index_column_name);

        /// Check all sizes are the same
        size_t dimensions = column_array_offsets[0];
        for (size_t i = 0; i < num_rows - 1; ++i)
            if (column_array_offsets[i + 1] - column_array_offsets[i] != dimensions)
                throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column '{}' must have equal length", index_column_name);

        /// Also check that previously inserted blocks have the same size as this block.
        /// Note that this guarantees consistency of dimensions only within parts. We are unable to detect inconsistent dimensions across
        /// parts - for this, a little help from the user is needed, e.g. CONSTRAINT cnstr CHECK length(array) = 42.
        if (index && index->dimensions() != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA, "All arrays in column '{}' must have equal length", index_column_name);

        if (!index)
            index = std::make_shared<USearchIndexWithSerialization>(dimensions, metric_kind, scalar_kind, usearch_hnsw_params);

        /// Add all rows of block
        if (!index->reserve(unum::usearch::ceil2(index->size() + num_rows)))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for usearch index");

        for (size_t row = 0; row < num_rows; ++row)
        {
            auto rc = index->add(static_cast<uint32_t>(index->size()), &column_array_data_float_data[column_array_offsets[row - 1]]);
            if (!rc)
                throw Exception::createRuntime(ErrorCodes::INCORRECT_DATA, rc.error.release());

            ProfileEvents::increment(ProfileEvents::USearchAddCount);
            ProfileEvents::increment(ProfileEvents::USearchAddVisitedMembers, rc.visited_members);
            ProfileEvents::increment(ProfileEvents::USearchAddComputedDistances, rc.computed_distances);
        }
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array(Float32) column");

    *pos += rows_read;
}

MergeTreeIndexConditionVectorSimilarity::MergeTreeIndexConditionVectorSimilarity(
    const IndexDescription & /*index_description*/,
    const SelectQueryInfo & query,
    unum::usearch::metric_kind_t metric_kind_,
    ContextPtr context)
    : condition(query, context)
    , metric_kind(metric_kind_)
{
}

bool MergeTreeIndexConditionVectorSimilarity::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /*idx_granule*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for vector indexes");
}

bool MergeTreeIndexConditionVectorSimilarity::alwaysUnknownOrTrue() const
{
    String distance_function;
    switch (metric_kind)
    {
        case unum::usearch::metric_kind_t::l2sq_k: distance_function = "L2Distance"; break;
        case unum::usearch::metric_kind_t::cos_k:  distance_function = "cosineDistance"; break;
        default: std::unreachable();
    }
    return condition.alwaysUnknownOrTrue(distance_function);
}

std::vector<size_t> MergeTreeIndexConditionVectorSimilarity::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    const UInt64 limit = condition.getLimit();
    const UInt64 index_granularity = condition.getIndexGranularity();
    const std::optional<float> comparison_distance = condition.getQueryType() == VectorSimilarityInfo::Type::Where
        ? std::optional<float>(condition.getComparisonDistanceForWhereQuery())
        : std::nullopt;

    if (comparison_distance && comparison_distance.value() < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to optimize query with where without distance");

    const std::vector<float> reference_vector = condition.getReferenceVector();

    const auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleVectorSimilarity>(idx_granule);
    if (granule == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule has the wrong type");

    const USearchIndexWithSerializationPtr index = granule->index;

    if (condition.getDimensions() != index->dimensions())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The dimension of the space in the request ({}) "
            "does not match the dimension in the index ({})",
            condition.getDimensions(), index->dimensions());

    auto result = index->search(reference_vector.data(), limit);

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, result.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, result.computed_distances);

    std::vector<UInt32> neighbors(result.size()); /// indexes of dots which were closest to the reference vector
    std::vector<Float32> distances(result.size());
    result.dump_to(neighbors.data(), distances.data());

    std::vector<size_t> granules;
    granules.reserve(neighbors.size());
    for (size_t i = 0; i < neighbors.size(); ++i)
    {
        if (comparison_distance && distances[i] > comparison_distance)
            continue;
        granules.push_back(neighbors[i] / index_granularity);
    }

    /// make unique
    std::sort(granules.begin(), granules.end());
    granules.erase(std::unique(granules.begin(), granules.end()), granules.end());

    return granules;
}

MergeTreeIndexVectorSimilarity::MergeTreeIndexVectorSimilarity(
        const IndexDescription & index_,
        unum::usearch::metric_kind_t metric_kind_,
        unum::usearch::scalar_kind_t scalar_kind_,
        UsearchHnswParams usearch_hnsw_params_)
    : IMergeTreeIndex(index_)
    , metric_kind(metric_kind_)
    , scalar_kind(scalar_kind_)
    , usearch_hnsw_params(usearch_hnsw_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarity::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarity>(index.name, index.sample_block, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarity::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarity>(index.name, index.sample_block, metric_kind, scalar_kind, usearch_hnsw_params);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionVectorSimilarity>(index, query, metric_kind, context);
};

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarity::createIndexCondition(const ActionsDAGPtr &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeIndexVectorSimilarity cannot be created with ActionsDAG");
}

MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index)
{
    const bool has_six_args = (index.arguments.size() == 6);

    unum::usearch::metric_kind_t metric_kind = nameToMetricKind.at(index.arguments[1].get<String>());

    /// use defaults for the other parameters
    unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::f32_k;
    UsearchHnswParams usearch_hnsw_params;

    if (has_six_args)
    {
        scalar_kind = nameToScalarKind.at(index.arguments[2].get<String>());
        usearch_hnsw_params = {.m               = index.arguments[3].get<UInt64>(),
                               .ef_construction = index.arguments[4].get<UInt64>(),
                               .ef_search       = index.arguments[5].get<UInt64>()};
    }

    return std::make_shared<MergeTreeIndexVectorSimilarity>(index, metric_kind, scalar_kind, usearch_hnsw_params);
}

void vectorSimilarityIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    const bool has_two_args = (index.arguments.size() == 2);
    const bool has_six_args = (index.arguments.size() == 6);

    /// Check number and type of arguments
    if (!has_two_args && !has_six_args)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector index must have two or six arguments");
    if (index.arguments[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "First argument of vector index (method) must be of type String");
    if (index.arguments[1].getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of vector index (metric) must be of type String");
    if (has_six_args)
    {
        if (index.arguments[2].getType() != Field::Types::String)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Third argument of vector index (quantization) must be of type String");
        if (index.arguments[3].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fourth argument of vector index (M) must be of type UInt64");
        if (index.arguments[4].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Fifth argument of vector index (ef_construction) must be of type UInt64");
        if (index.arguments[5].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Sixth argument of vector index (ef_search) must be of type UInt64");
    }

    /// Check that passed arguments are supported
    if (!methods.contains(index.arguments[0].get<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "First argument (method) of vector index is not supported. Supported kinds are: {}", joinByComma(methods));
    if (!nameToMetricKind.contains(index.arguments[1].get<String>()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Second argument (metric) of vector index is not supported. Supported kinds are: {}", joinByComma(nameToMetricKind));
    if (has_six_args)
    {
        if (!nameToScalarKind.contains(index.arguments[2].get<String>()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument (quantization) of vector index is not supported. Supported quantizations are: {}", joinByComma(nameToScalarKind));
        if (index.arguments[3].get<UInt64>() < 2)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Fourth argument (M) of vector index must be > 1");
        if (index.arguments[4].get<UInt64>() < 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Fifth argument (ef_construction) of vector index must be > 0");
        if (index.arguments[5].get<UInt64>() < 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Sixth argument (ef_search) of vector index must be > 0");
    }

    /// Check that the index is created on a single column
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Vector indexes must be created on a single column");

    /// Check data type of the indexed column:
    DataTypePtr data_type = index.sample_block.getDataTypes()[0];
    if (const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        TypeIndex nested_type_index = data_type_array->getNestedType()->getTypeId();
        if (!WhichDataType(nested_type_index).isFloat32())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector indexes can only be created on columns of type Array(Float32))");
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Vector indexes can only be created on columns of type Array(Float32))");
    }
}

}

#endif
