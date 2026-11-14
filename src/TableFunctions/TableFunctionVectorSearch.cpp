#include "config.h"

#if USE_GPU

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityIvf.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageValues.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 vector_search_ivf_probes;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
class TableFunctionVectorSearch : public ITableFunction
{
public:
    static constexpr auto name = "vectorSearch";
    std::string getName() const override { return name; }

    bool canBeUsedToCreateTable() const override { return false; }
    bool dependsOnCurrentUserGrants() const override { return true; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr, bool) const override { return structure(); }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription, bool) const override;
    const char * getStorageEngineName() const override { return ""; }

    static ColumnsDescription structure()
    {
        return ColumnsDescription{NamesAndTypesList{
            {"query", std::make_shared<DataTypeUInt32>()},
            {"part", std::make_shared<DataTypeString>()},
            {"part_offset", std::make_shared<DataTypeUInt64>()},
            {"distance", std::make_shared<DataTypeFloat32>()},
        }};
    }

    String database;
    String table;
    String index_name;
    size_t dimensions_of_queries = 0;
    size_t num_queries = 0;
    std::vector<float> queries; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t k = 0;
};

void TableFunctionVectorSearch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args = ast_function->children.at(0)->children;
    if (args.size() != 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' must have 5 arguments (database, table, index_name, queries, k), got: {}", getName(), args.size());

    database = checkAndGetLiteralArgument<String>(evaluateConstantExpressionForDatabaseName(args[0], context), "database");
    table = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context), "table");
    index_name = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context), "index_name");

    const Field queries_field = evaluateConstantExpression(args[3], context).first;
    if (queries_field.getType() != Field::Types::Array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The fourth argument of '{}' (queries) must be an Array(Array(Float*))", getName());
    const auto & queries_array = queries_field.safeGet<Array>();
    num_queries = queries_array.size();
    if (num_queries == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The fourth argument of '{}' (queries) must not be empty", getName());
    for (const auto & query : queries_array)
    {
        if (query.getType() != Field::Types::Array)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The fourth argument of '{}' (queries) must be an Array(Array(Float*))", getName());
        const auto & vector = query.safeGet<Array>();
        if (dimensions_of_queries == 0)
            dimensions_of_queries = vector.size();
        if (vector.size() != dimensions_of_queries || vector.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All queries of '{}' must have the same, non-zero number of dimensions", getName());
        for (const auto & value : vector)
            queries.push_back(static_cast<float>(applyVisitor(FieldVisitorConvertToNumber<Float64>(), value)));
    }

    k = checkAndGetLiteralArgument<UInt64>(evaluateConstantExpressionAsLiteral(args[4], context), "k");
    if (k == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The fifth argument of '{}' (k) must be positive", getName());
}

StoragePtr TableFunctionVectorSearch::executeImpl(
    const ASTPtr &, ContextPtr context, const std::string & table_name, ColumnsDescription, bool) const
{
    context->checkAccess(AccessType::SELECT, database, table);

    auto source = resolveStorageProxyLoading(DatabaseCatalog::instance().getTable(StorageID{database, table}, context));
    const auto * merge_tree = castStorage<MergeTreeData>(source, DeferredTable::Load).get();
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' expects a MergeTree table, got {}", getName(), source->getName());

    auto metadata = source->getInMemoryMetadataPtr(context, false);
    const auto & index_description = metadata->getSecondaryIndices().getByName(index_name);
    auto index = MergeTreeIndexFactory::instance().get(metadata, index_description, *merge_tree->getSettings());
    if (!typeid_cast<const MergeTreeIndexIvf *>(index.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index {} is not a vector_similarity index of the method 'ivf'", backQuote(index_name));

    const size_t ivf_probes = context->getSettingsRef()[Setting::vector_search_ivf_probes];
    if (ivf_probes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting 'vector_search_ivf_probes' must not be 0");

    auto mark_cache = context->getIndexMarkCache();
    auto uncompressed_cache = context->getIndexUncompressedCache();
    auto vector_cache = context->getVectorSimilarityIndexCache();
    auto reader_settings = MergeTreeReaderSettings::createFromContext(context);

    struct Candidate
    {
        float distance;
        UInt32 part;
        UInt64 offset;
    };
    VectorWithMemoryTracking<VectorWithMemoryTracking<Candidate>> candidates_per_query(num_queries);

    auto parts = merge_tree->getDataPartsVectorForInternalUsage();
    std::erase_if(parts, [](const auto & part) { return part->isEmpty(); });

    Stopwatch watch;
    size_t searched_granules = 0;
    std::vector<float> normalized; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<UInt32> neighbours; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<float> distances; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    for (UInt32 part_index = 0; part_index < parts.size(); ++part_index)
    {
        const auto & part = parts[part_index];
        auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>());
        if (!index->getDeserializedFormat(*part_info, index->getFileName()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Part {} has no index {}; materialize it first", part->name, backQuote(index_name));

        const auto & granularity = *part->index_granularity;
        const size_t index_granularity = index->index.granularity;
        const size_t marks_count = granularity.getMarksCountForSkipIndex(index_granularity);
        MarkRanges ranges{MarkRange(0, granularity.getMarksCountWithoutFinal())};
        MergeTreeIndexReader reader(index, part_info, marks_count, ranges, mark_cache.get(), uncompressed_cache.get(), vector_cache.get(),
            reader_settings, /*interruptible_marks_read=*/ true);

        for (size_t index_mark = 0; index_mark < marks_count; ++index_mark)
        {
            MergeTreeIndexGranulePtr granule_ptr;
            reader.read(index_mark, nullptr, granule_ptr, nullptr);
            if (granule_ptr->empty())
                continue;

            const auto & ivf = typeid_cast<const MergeTreeIndexGranuleIvf &>(*granule_ptr);
            if (ivf.dimensions != dimensions_of_queries)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The queries have {} dimensions, the index has {}", dimensions_of_queries, ivf.dimensions);

            if (normalized.empty())
            {
                normalized = queries;
                if (ivf.metric == IvfMetric::Cosine)
                    for (size_t q = 0; q < num_queries; ++q)
                        normalizeIvfVector(normalized.data() + q * dimensions_of_queries, dimensions_of_queries);
            }
            ivf.searchBatch(normalized, num_queries, k, ivf_probes, reader_settings, neighbours, distances);
            ++searched_granules;

            const size_t first_row = granularity.getMarkStartingRow(index_mark * index_granularity);
            for (size_t q = 0; q < num_queries; ++q)
                for (size_t i = 0; i < k; ++i)
                    if (neighbours[q * k + i] != std::numeric_limits<UInt32>::max())
                        candidates_per_query[q].push_back({distances[q * k + i], part_index, first_row + neighbours[q * k + i]});
        }
    }

    auto query_column = ColumnUInt32::create();
    auto part_column = ColumnString::create();
    auto offset_column = ColumnUInt64::create();
    auto distance_column = ColumnFloat32::create();

    for (size_t q = 0; q < num_queries; ++q)
    {
        auto & found = candidates_per_query[q];
        const size_t take = std::min(k, found.size());
        std::partial_sort(found.begin(), found.begin() + take, found.end(), [](const Candidate & a, const Candidate & b) { return a.distance < b.distance; });
        for (size_t i = 0; i < take; ++i)
        {
            query_column->insertValue(static_cast<UInt32>(q));
            part_column->insertData(parts[found[i].part]->name.data(), parts[found[i].part]->name.size());
            offset_column->insertValue(found[i].offset);
            distance_column->insertValue(found[i].distance);
        }
    }

    LOG_DEBUG(getLogger("vectorSearch"), "Searched {} queries in {} granules of {} parts in {} ms",
        num_queries, searched_granules, parts.size(), watch.elapsedMilliseconds());

    Block block{
        {std::move(query_column), std::make_shared<DataTypeUInt32>(), "query"},
        {std::move(part_column), std::make_shared<DataTypeString>(), "part"},
        {std::move(offset_column), std::make_shared<DataTypeUInt64>(), "part_offset"},
        {std::move(distance_column), std::make_shared<DataTypeFloat32>(), "distance"},
    };

    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), structure(), std::move(block));
    res->startup();
    return res;
}

}

void registerTableFunctionVectorSearch(TableFunctionFactory & factory);
void registerTableFunctionVectorSearch(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionVectorSearch>(
        {.description = R"(
The `k` nearest rows to each of several query vectors, searched in one batch on the GPU by a `vector_similarity` index of the `ivf` method. Every list of the index that any of the
queries probes (`vector_search_ivf_probes`) is read once for the whole batch.

```sql
SELECT query, part, part_offset, distance FROM vectorSearch(database, table, index_name, queries, k)
```

`queries` is a constant `Array(Array(Float32))`, for example `(SELECT groupArray(vec) FROM queries_table)`. The result has a row for
each of the `k` nearest rows of each query: the number of the query in `queries`, the part and the number of the row in it (as
`_part` and `_part_offset`), and the distance: the squared `L2Distance` or the `cosineDistance`, as the index is declared.
)", .category = FunctionDocumentation::Category::TableFunction},
        SecretArgumentsSpec{},
        {.allow_readonly = true});
}

}

#endif
