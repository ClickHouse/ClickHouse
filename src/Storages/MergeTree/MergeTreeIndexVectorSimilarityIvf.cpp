#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityIvf.h>

#include <Access/Common/AccessFlags.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Core/QualifiedTableName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IndicesDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageProxy.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <set>

#include <fmt/ranges.h>

#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
}

namespace
{

/// Positional arguments, as for HNSW: vector_similarity('ivf', 'distance_function', dimensions, <named arguments>)
constexpr size_t ARG_DISTANCE_FUNCTION = 1;
constexpr size_t ARG_DIMENSIONS = 2;
constexpr size_t NUM_POSITIONAL_ARGUMENTS = 3;

/// Named arguments
constexpr auto ARG_SAMPLES = "samples";
constexpr auto ARG_CLUSTERING = "clustering";
constexpr auto ARG_CLUSTER_STATISTICS = "cluster_statistics";

const std::set<String> ivf_distance_functions = {"L2Distance", "cosineDistance"};

Field getPositionalArgument(const ASTPtr & ast, size_t position)
{
    const auto * literal = ast->as<ASTLiteral>();
    if (!literal)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Argument {} of IVF vector similarity index must be a literal, got {}", position + 1, ast->formatForErrorMessage());
    return literal->value;
}

/// samples = 'db.table' or samples = db.table
String getSamplesTable(const ASTPtr & ast)
{
    String samples_table;
    if (const auto * literal = ast->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
        samples_table = literal->value.safeGet<String>();
    else if (const auto * identifier = ast->as<ASTIdentifier>())
        samples_table = identifier->name();
    else
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Argument '{}' of IVF vector similarity index must be a table name, got {}", ARG_SAMPLES, ast->formatForErrorMessage());

    if (samples_table.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Argument '{}' of IVF vector similarity index must not be empty", ARG_SAMPLES);
    return samples_table;
}

/// clustering = hierarchicalKMeans(k, ...): a call of an aggregate function with literal parameters
void getClustering(const ASTPtr & ast, String & clustering_function, Array & clustering_parameters)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Argument '{}' of IVF vector similarity index must be a call of a clustering function, e.g. hierarchicalKMeans(1024), got {}",
            ARG_CLUSTERING, ast->formatForErrorMessage());

    clustering_function = function->name;
    clustering_parameters.clear();
    for (const auto & parameter : function->arguments->children)
    {
        const auto * literal = parameter->as<ASTLiteral>();
        if (!literal)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Parameters of clustering function {} of IVF vector similarity index must be literals, got {}",
                clustering_function, parameter->formatForErrorMessage());
        clustering_parameters.push_back(literal->value);
    }

    /// By contract, the first parameter of every clustering function is the number of clusters k.
    if (clustering_parameters.empty() || clustering_parameters[0].getType() != Field::Types::UInt64 || clustering_parameters[0].safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "The first parameter of clustering function {} of IVF vector similarity index (number of clusters) must be a positive integer",
            clustering_function);
}

}

IvfIndexArguments parseVectorSimilarityIvfIndexArguments(const ASTPtr & arguments)
{
    const ASTs & children = arguments ? arguments->children : ASTs{};
    if (children.size() < NUM_POSITIONAL_ARGUMENTS)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "IVF vector similarity index must have the arguments 'ivf', the distance function and the dimensions, followed by named "
            "arguments, e.g. vector_similarity('ivf', 'L2Distance', 768, samples = 'samples', clustering = hierarchicalKMeans(1024))");

    IvfIndexArguments result;

    const Field distance_function = getPositionalArgument(children[ARG_DISTANCE_FUNCTION], ARG_DISTANCE_FUNCTION);
    if (distance_function.getType() != Field::Types::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Second argument of IVF vector similarity index (distance function) must be of type String");

    result.distance_function = distance_function.safeGet<String>();

    if (!ivf_distance_functions.contains(result.distance_function))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Second argument (distance function) of IVF vector similarity index is not supported. Supported distance functions are: {}",
            fmt::join(ivf_distance_functions, ", "));

    const Field dimensions = getPositionalArgument(children[ARG_DIMENSIONS], ARG_DIMENSIONS);
    if (dimensions.getType() != Field::Types::UInt64 || dimensions.safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Third argument of IVF vector similarity index (dimensions) must be a positive integer");
    result.dimensions = dimensions.safeGet<UInt64>();

    bool has_samples = false;
    bool has_clustering = false;
    bool has_cluster_statistics = false;
    for (size_t i = NUM_POSITIONAL_ARGUMENTS; i < children.size(); ++i)
    {
        const auto * equals = children[i]->as<ASTFunction>();
        if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2
            || !equals->arguments->children[0]->as<ASTIdentifier>())
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Arguments of IVF vector similarity index after the dimensions must be named, e.g. samples = 'samples', got {}",
                children[i]->formatForErrorMessage());

        const String key = equals->arguments->children[0]->as<ASTIdentifier &>().name();
        const ASTPtr & value = equals->arguments->children[1];

        if (key == ARG_SAMPLES)
        {
            if (has_samples)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Argument '{}' of IVF vector similarity index is specified more than once", key);
            result.samples_table = getSamplesTable(value);
            has_samples = true;
        }
        else if (key == ARG_CLUSTERING)
        {
            if (has_clustering)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Argument '{}' of IVF vector similarity index is specified more than once", key);
            getClustering(value, result.clustering_function, result.clustering_parameters);
            has_clustering = true;
        }
        else if (key == ARG_CLUSTER_STATISTICS)
        {
            if (has_cluster_statistics)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Argument '{}' of IVF vector similarity index is specified more than once", key);
            const auto * literal = value->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::UInt64 || literal->value.safeGet<UInt64>() > 1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Argument '{}' of IVF vector similarity index must be 0 or 1", key);
            result.cluster_statistics = literal->value.safeGet<UInt64>() == 1;
            has_cluster_statistics = true;
        }
        else
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Unknown argument '{}' of IVF vector similarity index. Supported named arguments are: {}, {}, {}",
                key, ARG_SAMPLES, ARG_CLUSTERING, ARG_CLUSTER_STATISTICS);
    }

    if (!has_samples)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "IVF vector similarity index requires argument '{}'", ARG_SAMPLES);
    if (!has_clustering)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "IVF vector similarity index requires argument '{}'", ARG_CLUSTERING);

    return result;
}

namespace
{

/// An unqualified samples table name refers to the database of the table.
QualifiedTableName resolveSamplesTable(const IvfIndexArguments & arguments, const String & current_database)
{
    auto samples_table = QualifiedTableName::parseFromString(arguments.samples_table);
    if (samples_table.database.empty())
        samples_table.database = current_database;
    return samples_table;
}

}

bool isVectorSimilarityIvfIndex(const IndexDescription & index)
{
    /// Only the first argument (the method) is inspected: the named arguments of IVF are not plain literals.
    if (index.type != "vector_similarity" || !index.arguments || index.arguments->children.empty())
        return false;
    const auto * method = index.arguments->children[0]->as<ASTLiteral>();
    return method && method->value.getType() == Field::Types::String && method->value.safeGet<String>() == "ivf";
}

MergeTreeIndexGranuleVectorSimilarityIvf::MergeTreeIndexGranuleVectorSimilarityIvf(IvfClusterCounts && cluster_counts_)
    : cluster_counts(std::move(cluster_counts_))
{
}

void MergeTreeIndexGranuleVectorSimilarityIvf::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty IVF index");

    writeIntBinary(FILE_FORMAT_VERSION, ostr);
    writeVarUInt(cluster_counts.size(), ostr);
    for (const auto & [cluster_id, count] : cluster_counts)
    {
        writeBinaryLittleEndian(cluster_id, ostr);
        writeVarUInt(count, ostr);
    }
}

void MergeTreeIndexGranuleVectorSimilarityIvf::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt64 file_version = 0;
    readIntBinary(file_version, istr);
    if (file_version != FILE_FORMAT_VERSION)
        throw Exception(ErrorCodes::FORMAT_VERSION_TOO_OLD,
            "IVF index could not be loaded because its version is too old (current version: {}, persisted version: {}). Please drop the index and create it again.",
            FILE_FORMAT_VERSION, file_version);

    UInt64 size = 0;
    readVarUInt(size, istr);

    cluster_counts.clear();
    for (UInt64 i = 0; i < size; ++i)
    {
        IvfClusterId cluster_id = 0;
        UInt64 count = 0;
        readBinaryLittleEndian(cluster_id, istr);
        readVarUInt(count, istr);
        cluster_counts.emplace_hint(cluster_counts.end(), cluster_id, count);
    }
}

MergeTreeIndexAggregatorVectorSimilarityIvf::MergeTreeIndexAggregatorVectorSimilarityIvf(
    const String & vector_column_, const String & cluster_id_column_, UInt64 dimensions_)
    : vector_column(vector_column_)
    , cluster_id_column(cluster_id_column_)
    , dimensions(dimensions_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorVectorSimilarityIvf::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleVectorSimilarityIvf>(std::move(cluster_counts));
    cluster_counts = {};
    return granule;
}

/// Just maintains cluster statistics : <cid, count> pairs
void MergeTreeIndexAggregatorVectorSimilarityIvf::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);
    const size_t end = *pos + rows_read;

    /// All vectors must have the declared number of dimensions, as for HNSW.
    const auto & vectors = block.getByName(vector_column).column;
    const auto * column_array = typeid_cast<const ColumnArray *>(vectors.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array column in IVF index, got {}", vectors->getName());
    const auto & offsets = column_array->getOffsets();
    for (size_t row = *pos; row < end; ++row)
    {
        const size_t vector_dimensions = offsets[row] - (row ? offsets[row - 1] : 0);
        if (vector_dimensions != dimensions)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Array values in column with IVF vector similarity index have {} elements, expects {} elements", vector_dimensions, dimensions);
    }

    const auto & column = block.getByName(cluster_id_column).column;
    const auto * column_cluster_id = typeid_cast<const ColumnUInt32 *>(column.get());
    if (!column_cluster_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected UInt32 cluster id column in IVF index, got {}", column->getName());

    const auto & data = column_cluster_id->getData();

    /// The table is sorted by the cluster id, so consecutive rows mostly share the same cluster id.
    size_t row = *pos;
    while (row < end)
    {
        const IvfClusterId cluster_id = data[row];
        size_t run_end = row + 1;
        while (run_end < end && data[run_end] == cluster_id)
            ++run_end;
        cluster_counts[cluster_id] += run_end - row;
        row = run_end;
    }

    *pos += rows_read;
}

MergeTreeIndexVectorSimilarityIvf::MergeTreeIndexVectorSimilarityIvf(
    StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, IvfIndexArguments arguments_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , arguments(std::move(arguments_))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexVectorSimilarityIvf::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleVectorSimilarityIvf>();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexVectorSimilarityIvf::createIndexAggregator() const
{
    /// Validated by vectorSimilarityIvfIndexValidator: the index is built on (vector, cluster id).
    return std::make_shared<MergeTreeIndexAggregatorVectorSimilarityIvf>(
        index.sample_block.getByPosition(0).name, index.sample_block.getByPosition(1).name, arguments.dimensions);
}

MergeTreeIndexConditionPtr MergeTreeIndexVectorSimilarityIvf::createIndexCondition(const ActionsDAG::Node * /*predicate*/, ContextPtr /*context*/) const
{
    return std::make_shared<MergeTreeIndexConditionVectorSimilarityIvf>();
}

MergeTreeIndexPtr vectorSimilarityIvfIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & /*settings*/)
{
    return std::make_shared<MergeTreeIndexVectorSimilarityIvf>(
        std::move(metadata_snapshot), index, parseVectorSimilarityIvfIndexArguments(index.arguments));
}

void vectorSimilarityIvfIndexValidator(const IndexDescription & index, bool /*attach*/, const MergeTreeSettings & /*settings*/)
{
    /// Check number and type of arguments. The clustering parameters after k are specific to the clustering function and
    /// are validated by the function itself (see checkVectorSimilarityIvfIndexForTable).
    parseVectorSimilarityIvfIndexArguments(index.arguments);

    /// The index name is part of the names of its hidden objects, which must not contain dots.
    if (index.name.contains('.'))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The name of an IVF vector similarity index must not contain dots");

    /// Check that the index is created on (vector, cluster id)
    if (index.column_names.size() != 2 || index.data_types.size() != 2)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "IVF vector similarity index must be created on two columns: (vector, cluster id)");

    /// Check that the vector column is Array(Float32|Float64|BFloat16)
    const DataTypePtr & vector_data_type = index.data_types[0];
    const auto * data_type_array = typeid_cast<const DataTypeArray *>(vector_data_type.get());
    if (!data_type_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First column of IVF vector similarity index must be of type Array(Float32|Float64|BFloat16)");
    WhichDataType which_nested(data_type_array->getNestedType()->getTypeId());
    if (!which_nested.isNativeFloat() && !which_nested.isBFloat16())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First column of IVF vector similarity index must be of type Array(Float32|Float64|BFloat16)");

    /// Check that the cluster id column is UInt32
    if (!WhichDataType(index.data_types[1]).isUInt32())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second column of IVF vector similarity index (cluster id) must be of type UInt32, got {}", index.data_types[1]->getName());
}

namespace
{

/// Contract of clustering functions: the result is either the leaf centroids, Array(Array(Float32)), or the tree of centroids,
/// Array(Tuple(vec Array(Float32), level UInt8, cid UInt32, parent UInt32[, radius Float32])) with named elements in any order.
struct IvfClusteringResult
{
    bool is_tree = false;
    bool has_radius = false;
};

IvfClusteringResult checkClusteringResultType(const DataTypePtr & type, const String & clustering_function, const String & index_name)
{
    const auto float32 = std::make_shared<DataTypeFloat32>();
    const auto vector = std::make_shared<DataTypeArray>(float32);
    auto throw_unexpected = [&]
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Clustering function {} of IVF vector similarity index {} must return Array(Array(Float32)) or "
            "Array(Tuple(vec Array(Float32), level UInt8, cid UInt32, parent UInt32[, radius Float32])), but returns {}",
            clustering_function, backQuoteIfNeed(index_name), type->getName());
    };

    const auto * array = typeid_cast<const DataTypeArray *>(type.get());
    if (!array)
        throw_unexpected();
    if (array->getNestedType()->equals(*vector))
        return {};

    const auto * tuple = typeid_cast<const DataTypeTuple *>(array->getNestedType().get());
    if (!tuple || !tuple->hasExplicitNames())
        throw_unexpected();

    const std::map<String, DataTypePtr> expected_elements = {
        {"vec", vector}, {"level", std::make_shared<DataTypeUInt8>()}, {"cid", std::make_shared<DataTypeUInt32>()},
        {"parent", std::make_shared<DataTypeUInt32>()}, {"radius", float32}};
    IvfClusteringResult result{.is_tree = true};
    std::set<String> seen;
    for (size_t i = 0; i < tuple->getElements().size(); ++i)
    {
        const String & name = tuple->getElementNames()[i];
        const auto expected = expected_elements.find(name);
        if (expected == expected_elements.end() || !expected->second->equals(*tuple->getElements()[i]) || !seen.insert(name).second)
            throw_unexpected();
    }
    if (seen.size() < 4 || (seen.size() == 4 && seen.contains("radius")))
        throw_unexpected();
    result.has_radius = seen.contains("radius");
    return result;
}

/// Instantiates the clustering function of an index on the vector column of its samples table. This runs the function's own
/// validation of its parameters and of the argument type, so the index does not need to know the specifics of each algorithm.
std::pair<StoragePtr, AggregateFunctionPtr> getClusteringFunction(
    const IndexDescription & index, const IvfIndexArguments & arguments, const QualifiedTableName & samples_table, ContextPtr context)
{
    auto samples_storage = DatabaseCatalog::instance().tryGetTable(StorageID(samples_table.database, samples_table.table), context);
    if (!samples_storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Samples table {} of IVF vector similarity index {} does not exist",
            samples_table.getFullName(), backQuoteIfNeed(index.name));

    /// The samples table must have a vector column with the same name as the indexed vector column.
    const String & vector_column = index.column_names[0];
    const auto samples_metadata = samples_storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    const auto samples_vector = samples_metadata->getColumns().tryGetPhysical(vector_column);
    if (!samples_vector)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "Samples table {} of IVF vector similarity index {} has no column {}",
            samples_table.getFullName(), backQuoteIfNeed(index.name), backQuoteIfNeed(vector_column));

    AggregateFunctionProperties properties;
    auto aggregate_function = AggregateFunctionFactory::instance().get(
        arguments.clustering_function, NullsAction::EMPTY, {samples_vector->type}, arguments.clustering_parameters, properties);
    return {samples_storage, aggregate_function};
}

}

void checkVectorSimilarityIvfIndexForTable(
    const IndexDescription & index,
    const KeyDescription & primary_key,
    bool check_samples_table,
    const String & current_database,
    ContextPtr context)
{
    /// cid IN (...) prunes only through the primary index.
    const String & cluster_id_column = index.column_names[1];
    const auto & primary_key_columns = primary_key.column_names;
    if (std::find(primary_key_columns.begin(), primary_key_columns.end(), cluster_id_column) == primary_key_columns.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cluster id column {} of IVF vector similarity index {} must be a column of the primary key",
            backQuoteIfNeed(cluster_id_column), backQuoteIfNeed(index.name));

    if (check_samples_table)
    {
        const IvfIndexArguments arguments = parseVectorSimilarityIvfIndexArguments(index.arguments);
        const auto samples_table = resolveSamplesTable(arguments, current_database);

        const auto [samples_storage, aggregate_function] = getClusteringFunction(index, arguments, samples_table, context);
        checkClusteringResultType(aggregate_function->getResultType(), arguments.clustering_function, index.name);

        /// Training k centroids needs at least k samples.
        const UInt64 k = arguments.clustering_parameters[0].safeGet<UInt64>();
        const auto samples_rows = samples_storage->totalRows(context);
        if (!samples_rows)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot determine the number of rows of samples table {} of IVF vector similarity index {}",
                samples_table.getFullName(), backQuoteIfNeed(index.name));
        if (*samples_rows < k)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Samples table {} of IVF vector similarity index {} has {} rows, but at least {} (the number of clusters) are required",
                samples_table.getFullName(), backQuoteIfNeed(index.name), *samples_rows, k);
    }
}

namespace
{

String getInnerObjectName(const String & kind, const StorageID & table_id, const String & index_name)
{
    /// No dots: names with several dots can not be passed as strings, e.g. the dictionary name to assignCentroid.
    return fmt::format("_{}_{}_{}", kind, index_name, toString(table_id.uuid));
}

/// Like InterpreterDropQuery::executeDropQuery, which can not drop dictionaries: DROP TABLE refuses them.
void dropInnerObject(const StorageID & inner_id, bool is_dictionary, ContextPtr context)
{
    if (!DatabaseCatalog::instance().tryGetTable(inner_id, context))
        return;

    auto drop_query = make_intrusive<ASTDropQuery>();
    drop_query->setDatabase(inner_id.getDatabaseName());
    drop_query->setTable(inner_id.getTableName());
    drop_query->kind = ASTDropQuery::Kind::Drop;
    drop_query->is_dictionary = is_dictionary;
    drop_query->if_exists = true;
    drop_query->sync = false;

    /// The hidden objects are dropped together with the table even if the user has no grants on them,
    /// as for the inner table of a materialized view.
    auto drop_context = Context::createCopy(context->getGlobalContext());
    drop_context->setSettings(context->getSettingsRef());
    drop_context->setSetting("database_atomic_wait_for_drop_and_detach_synchronously", false);

    /// The objects depend on each other and the table depends on them (see getLoadingDependenciesFromCreateQuery); the
    /// dependencies only protect them from being dropped on their own and must not stop dropping them with the table.
    drop_context->setSetting("check_table_dependencies", false);
    drop_context->setSetting("check_referential_table_dependencies", false);
    drop_context->setDDLOrOnClusterInternal(true);

    ASTPtr ast = drop_query;
    InterpreterDropQuery(ast, drop_context).execute();
}

void executeInternalQuery(const String & query, ContextPtr context)
{
    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId({});
    auto io = executeQuery(query, query_context, QueryFlags{.internal = true}).second;
    executeTrivialBlockIO(io, query_context);
}

}

StorageID getVectorSimilarityIvfCentroidsTableID(const StorageID & table_id, const String & index_name)
{
    return StorageID(table_id.getDatabaseName(), getInnerObjectName("ivf_quantizer", table_id, index_name));
}

StorageID getVectorSimilarityIvfCentroidsDictionaryID(const StorageID & table_id, const String & index_name)
{
    return StorageID(table_id.getDatabaseName(), getInnerObjectName("ivf_centroids_dict", table_id, index_name));
}

std::optional<IvfCentroidsDictionary> tryGetVectorSimilarityIvfCentroidsDictionary(const String & name, const ContextPtr & context)
{
    auto qualified_name = QualifiedTableName::tryParseFromString(name);
    if (!qualified_name)
        return {};

    /// The dictionary of an IVF index is found through the table UUID in its name, in the current database of the table. So a
    /// reference to it (e.g. in the DEFAULT expression of the cluster id column) is not affected by RENAME or EXCHANGE.
    static constexpr std::string_view dictionary_prefix = "_ivf_centroids_dict_";
    static constexpr size_t uuid_length = 36;
    const String & object_name = qualified_name->table;
    if (object_name.starts_with(dictionary_prefix))
    {
        UUID table_uuid;
        if (object_name.size() <= dictionary_prefix.size() + uuid_length + 1 || object_name[object_name.size() - uuid_length - 1] != '_'
            || !tryParse(table_uuid, object_name.substr(object_name.size() - uuid_length)))
            return {};
        auto [database, table] = DatabaseCatalog::instance().tryGetByUUID(table_uuid);
        if (!database || !table)
            return {};
        return IvfCentroidsDictionary{database->getDatabaseName() + "." + object_name, table->getStorageID()};
    }

    if (qualified_name->database.empty())
        qualified_name->database = context->getCurrentDatabase();

    /// Only MergeTree tables can have an IVF index; the metadata of other storages may resolve other tables (e.g. aliases).
    auto table = DatabaseCatalog::instance().tryGetTable(StorageID(qualified_name->database, qualified_name->table), context);
    if (!castStorage<MergeTreeData>(table, DeferredTable::Load))
        return {};

    const auto metadata = table->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    for (const auto & index : metadata->getSecondaryIndices())
        if (isVectorSimilarityIvfIndex(index))
            return IvfCentroidsDictionary{
                getVectorSimilarityIvfCentroidsDictionaryID(table->getStorageID(), index.name).getFullNameNotQuoted(), table->getStorageID()};
    return {};
}

bool isSharedCatalogReplay([[maybe_unused]] const ContextPtr & context)
{
#if CLICKHOUSE_CLOUD
    return context->getClientInfo().is_shared_catalog_internal && !SharedDatabaseCatalog::isInitialQuery(context);
#else
    return false;
#endif
}

void createVectorSimilarityIvfInnerObjects(
    const StorageID & table_id, const String & table_engine, const StorageInMemoryMetadata & metadata, ContextPtr context)
{
    if (isSharedCatalogReplay(context))
        return;

    const String centroids_engine = table_engine.starts_with("Shared") ? "SharedMergeTree" : "MergeTree";

    /// Like the inner table of a materialized view, the hidden objects belong to the table: they are created without checking
    /// the grants of the user on them, and are used through the grants on the table.
    auto internal_context = Context::createCopy(context->getGlobalContext());
    internal_context->applySettingsChanges(context->getSettingsRef().changes());

    for (const auto & index : metadata.secondary_indices)
    {
        if (!isVectorSimilarityIvfIndex(index))
            continue;

        /// The internal objects are named after the table UUID, so that they survive RENAME of the table.
        if (table_id.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "IVF vector similarity index {} is only supported in databases with table UUIDs (e.g. Atomic)", backQuoteIfNeed(index.name));

        const IvfIndexArguments arguments = parseVectorSimilarityIvfIndexArguments(index.arguments);
        const StorageID centroids_table_id = getVectorSimilarityIvfCentroidsTableID(table_id, index.name);
        const StorageID centroids_dictionary_id = getVectorSimilarityIvfCentroidsDictionaryID(table_id, index.name);

        /// The training runs internally (see scheduleVectorSimilarityIvfTraining), so the user who creates the table must be
        /// allowed to read the samples it uses.
        const auto samples_table = resolveSamplesTable(arguments, table_id.getDatabaseName());
        context->checkAccess(AccessType::SELECT, samples_table.database, samples_table.table, index.column_names[0]);

        /// The HNSW index speeds up the search over many centroids. Without USearch, the centroids are scanned.
#if USE_USEARCH
        const String hnsw_index = fmt::format(
            ", INDEX vec_hnsw vec TYPE vector_similarity('hnsw', {}, {}, 'f32', 64, 256)",
            quoteString(arguments.distance_function), arguments.dimensions);
#else
        const String hnsw_index;
#endif

        /// One partition per level, since HNSW is built per part. Rows are ordered by parent, so the children of the chosen
        /// centroids are read by primary key. Centroids are wide, hence the small granularity.
        executeInternalQuery(fmt::format(
            "CREATE TABLE {} "
            "(level UInt8, cid UInt32, parent UInt32, vec Array(Float32), active Bool, radius Float32, "
            "generation UInt32, generation_size UInt32{}) "
            "ENGINE = {} PARTITION BY level ORDER BY (level, parent, cid) SETTINGS index_granularity = 256",
            centroids_table_id.getFullTableName(), hnsw_index, centroids_engine), internal_context);

        executeInternalQuery(fmt::format(
            "CREATE DICTIONARY {} "
            "(level UInt8, cid UInt32, parent UInt32, vec Array(Float32), active Bool, generation UInt32, generation_size UInt32) "
            "PRIMARY KEY level, cid "
            "SOURCE(CLICKHOUSE(DB {} TABLE {})) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0)",
            centroids_dictionary_id.getFullTableName(),
            quoteString(centroids_table_id.getDatabaseName()), quoteString(centroids_table_id.getTableName())), internal_context);
    }
}

Strings getVectorSimilarityIvfIndexNames(const StorageInMemoryMetadata & metadata)
{
    Strings index_names;
    for (const auto & index : metadata.secondary_indices)
        if (isVectorSimilarityIvfIndex(index))
            index_names.push_back(index.name);
    return index_names;
}

Strings getVectorSimilarityIvfIndexNames(const ASTCreateQuery & create)
{
    Strings index_names;
    if (!create.columns_list || !create.columns_list->indices)
        return index_names;
    for (const auto & child : create.columns_list->indices->children)
    {
        const auto * index = child->as<ASTIndexDeclaration>();
        const auto type = index ? index->getType() : nullptr;
        if (!type || type->name != "vector_similarity" || !type->arguments || type->arguments->children.empty())
            continue;
        const auto * method = type->arguments->children[0]->as<ASTLiteral>();
        if (method && method->value == Field("ivf"))
            index_names.push_back(index->name);
    }
    return index_names;
}

void dropVectorSimilarityIvfInnerObjects(const StorageID & table_id, const Strings & index_names, ContextPtr context)
{
    if (table_id.uuid == UUIDHelpers::Nil || isSharedCatalogReplay(context))
        return;

    for (const auto & index_name : index_names)
    {
        /// The dictionary reads from the table, so it goes first. Like the inner table of a materialized view: no DDL guards
        /// (they must be taken in name order) and no synchronous drop (it may deadlock in the background drop task).
        dropInnerObject(getVectorSimilarityIvfCentroidsDictionaryID(table_id, index_name), /*is_dictionary=*/ true, context);
        dropInnerObject(getVectorSimilarityIvfCentroidsTableID(table_id, index_name), /*is_dictionary=*/ false, context);
    }
}

void scheduleVectorSimilarityIvfTraining(const StorageID & table_id, const StorageInMemoryMetadata & metadata, ContextPtr context)
{
    if (isSharedCatalogReplay(context))
        return;

    for (const auto & index : metadata.secondary_indices)
    {
        if (!isVectorSimilarityIvfIndex(index))
            continue;

        const IvfIndexArguments arguments = parseVectorSimilarityIvfIndexArguments(index.arguments);
        const auto samples_table = resolveSamplesTable(arguments, table_id.getDatabaseName());
        const StorageID centroids_table_id = getVectorSimilarityIvfCentroidsTableID(table_id, index.name);
        const StorageID centroids_dictionary_id = getVectorSimilarityIvfCentroidsDictionaryID(table_id, index.name);

        Strings clustering_parameters;
        for (const auto & parameter : arguments.clustering_parameters)
            clustering_parameters.push_back(applyVisitor(FieldVisitorToString(), parameter));

        const auto [samples_storage, aggregate_function] = getClusteringFunction(index, arguments, samples_table, context);
        const auto result_form = checkClusteringResultType(aggregate_function->getResultType(), arguments.clustering_function, index.name);

        /// Both forms of the result become (vec, level, cid, parent, radius) tuples; plain centroids are leaves numbered from 1.
        String nodes;
        if (result_form.is_tree)
            nodes = fmt::format(
                "arrayMap(c -> (tupleElement(c, 'vec'), tupleElement(c, 'level'), tupleElement(c, 'cid'), tupleElement(c, 'parent'), {}), result)",
                result_form.has_radius ? "tupleElement(c, 'radius')" : "toFloat32(0)");
        else
            nodes = "arrayMap((c, i) -> (c, toUInt8(0), toUInt32(i), toUInt32(0), toFloat32(0)), result, arrayEnumerate(result))";

        /// For cosine distance the centroids are normalized, because rows are assigned by L2 distance, which agrees with cosine
        /// distance only for unit vectors.
        const String centroid = arguments.distance_function == "cosineDistance" ? "L2Normalize(node.1)" : "node.1";

        /// The result is checked as a whole before anything is written, so the table keeps reading all rows instead of using
        /// wrong centroids: k leaves (level 0), the declared dimensions, unique non-zero ids per level, and every centroid below
        /// the top level has its parent one level up (0 = no parent). All rows are generation 0.
        const UInt64 num_clusters = arguments.clustering_parameters[0].safeGet<UInt64>();
        const String insert_query = fmt::format(
            "INSERT INTO {0} (level, cid, parent, vec, active, radius, generation, generation_size) "
            "SELECT node.2, node.3, node.4, {1}, true, node.5, 0, length(nodes) "
            "FROM (SELECT {2} AS nodes, arrayMap(n -> (toUInt16(n.2), n.3), nodes) AS keys, arrayMax(n -> n.2, nodes) AS top "
            "FROM (SELECT {3}({4})({5}) AS result FROM {6}) "
            "WHERE throwIf(NOT arrayAll(n -> length(n.1) = {7}, nodes), "
            "'The clustering function must return centroids with {7} dimensions') "
            "+ throwIf(countEqual(arrayMap(n -> n.2, nodes), 0) != {8}, "
            "'The clustering function must return {8} centroids at level 0') "
            "+ throwIf(NOT arrayAll(n -> n.3 != 0 AND n.2 < 255, nodes) OR length(arrayDistinct(keys)) != length(nodes), "
            "'The clustering function must return unique non-zero centroid ids per level and levels below 255') "
            "+ throwIf(NOT arrayAll(n -> n.2 < top OR n.4 = 0, nodes) "
            "OR length(arrayDistinct(arrayConcat(keys, arrayMap(n -> (toUInt16(n.2 + 1), n.4), arrayFilter(n -> n.2 < top, nodes))))) != length(arrayDistinct(keys)), "
            "'The clustering function must return a parent one level up for each centroid below the top level, and parent 0 at the top level') = 0) "
            "ARRAY JOIN nodes AS node",
            centroids_table_id.getFullTableName(), centroid, nodes,
            backQuoteIfNeed(arguments.clustering_function), fmt::join(clustering_parameters, ", "),
            backQuoteIfNeed(index.column_names[0]), samples_storage->getStorageID().getFullTableName(),
            arguments.dimensions, num_clusters);
        const String reload_query = fmt::format("SYSTEM RELOAD DICTIONARY {}", centroids_dictionary_id.getFullTableName());

        /// The training runs after CREATE returns and outlives the session, so it gets a context of its own, with the settings
        /// of the query which created the table. It writes the hidden objects, which need no grants of the user (see
        /// createVectorSimilarityIvfInnerObjects, which also checks that the user may read the samples).
        auto training_context = Context::createCopy(context->getGlobalContext());
        training_context->applySettingsChanges(context->getSettingsRef().changes());
        const String index_name = index.name;
        const String table_name = table_id.getNameForLogs();

        context->getBackgroundQueryPool().scheduleOrThrow([training_context, insert_query, reload_query, index_name, table_name]
        {
            auto log = getLogger("VectorSimilarityIvfTraining");
            try
            {
                auto thread_group = ThreadGroup::createForQuery(training_context);
                ThreadGroupSwitcher switcher(thread_group, ThreadName::BACKGROUND_QUERY);

                LOG_INFO(log, "Training centroids of IVF index {} of table {}: {}", index_name, table_name, insert_query);
                Stopwatch watch;
                executeInternalQuery(insert_query, training_context);
                executeInternalQuery(reload_query, training_context);
                LOG_INFO(log, "Trained centroids of IVF index {} of table {} in {:.3f} sec", index_name, table_name, watch.elapsedSeconds());
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Failed to train centroids of IVF index {} of table {}", index_name, table_name));
            }
        });
    }
}

bool rewriteVectorSimilarityIvfSelfReferences(const StorageID & table_id, const Strings & index_names, ColumnsDescription & columns)
{
    if (table_id.uuid == UUIDHelpers::Nil || index_names.size() != 1)
        return false;
    const String dictionary_name = getVectorSimilarityIvfCentroidsDictionaryID(table_id, index_names.front()).getTableName();

    /// Replaces the name of this table in assignCentroid(..., 'table') by the name of its centroids dictionary.
    std::function<bool(const ASTPtr &)> rewrite = [&](const ASTPtr & ast)
    {
        bool changed = false;
        if (const auto * function = ast->as<ASTFunction>(); function && function->name == "assignCentroid" && function->arguments
            && function->arguments->children.size() == 2)
        {
            if (auto * literal = function->arguments->children[1]->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
            {
                auto name = QualifiedTableName::tryParseFromString(literal->value.safeGet<String>());
                if (name && name->table == table_id.getTableName() && (name->database.empty() || name->database == table_id.getDatabaseName()))
                {
                    literal->value = dictionary_name;
                    changed = true;
                }
            }
        }
        for (const auto & child : ast->children)
            changed |= rewrite(child);
        return changed;
    };

    std::vector<std::pair<String, ASTPtr>> rewritten;
    for (const auto & column : columns)
    {
        if (!column.default_desc.expression)
            continue;
        ASTPtr expression = column.default_desc.expression->clone();
        if (rewrite(expression))
            rewritten.emplace_back(column.name, expression);
    }
    for (const auto & [name, expression] : rewritten)
        columns.modify(name, [&](ColumnDescription & description) { description.default_desc.expression = expression; });
    return !rewritten.empty();
}

}
