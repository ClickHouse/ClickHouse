#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageFilesystemCache.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

namespace
{

/** filesystemCache(cache_name, max_elements, max_total_size[, order]) - the file segments of a filesystem cache,
  * selected by their priority. See `IFileCachePriority::observeFilesystemCache`.
  * order is optional
  */
class TableFunctionFilesystemCache : public ITableFunction
{
public:
    static constexpr auto name = "filesystemCache";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

    /// `random` draws a new sample on every read.
    bool isDeterministicInScopeOfQuery() const override { return query_order != IFileCachePriority::IterationOrder::RANDOM; }

private:
    /// No underlying storage engine
    const char * getStorageEngineName() const override { return ""; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const override
    {
        return StorageFilesystemCache::getColumnsDescription();
    }

    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const override;

    String cache_name;
    UInt64 query_max_elements = 0;
    UInt64 query_max_total_size = 0;
    IFileCachePriority::IterationOrder query_order = IFileCachePriority::IterationOrder::HOTTEST;
};

IFileCachePriority::IterationOrder parseOrder(const String & order)
{
    if (order == "hottest")
        return IFileCachePriority::IterationOrder::HOTTEST;
    if (order == "coldest")
        return IFileCachePriority::IterationOrder::COLDEST;
    if (order == "random")
        return IFileCachePriority::IterationOrder::RANDOM;
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown order '{}' in table function `filesystemCache`, expected 'hottest', 'coldest' or 'random'",
        order);
}

void TableFunctionFilesystemCache::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Wrong AST structure in table function '{}'.", getName());

    ASTs & args = args_func.at(0)->children;
    if (args.size() < 3 || args.size() > 4)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires 3 or 4 arguments: cache_name, max_elements, max_total_size[, order]",
            getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    cache_name = checkAndGetLiteralArgument<String>(args[0], "cache_name");
    query_max_elements = checkAndGetLiteralArgument<UInt64>(args[1], "max_elements");
    query_max_total_size = checkAndGetLiteralArgument<UInt64>(args[2], "max_total_size");
    if (args.size() == 4)
        query_order = parseOrder(checkAndGetLiteralArgument<String>(args[3], "order"));
}

StoragePtr TableFunctionFilesystemCache::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /* cached_columns */,
    bool is_insert_query) const
{
    auto storage = std::make_shared<StorageFilesystemCache>(
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context, is_insert_query),
        cache_name,
        query_max_elements,
        query_max_total_size,
        query_order);
    storage->startup();
    return storage;
}

}

void registerTableFunctionFilesystemCache(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFilesystemCache>(
        {
            .description = R"(Returns the file segments of a filesystem cache, selected by their position in the eviction queue of the cache.

With 'hottest' and 'coldest', the queue is walked from one end and the walk stops as soon as `max_elements` segments have been returned or the next segment would make the total `size` exceed `max_total_size`: that segment is not returned, and neither are the ones after it, even if they would fit.

With 'random', the whole queue is walked to take a uniform sample of at most `max_elements` segments, ignoring `max_total_size`. The sample is then shuffled, and its segments are taken in that order until the next one would bring the total `size` over `max_total_size`, so the result can be smaller than what would fit.

A cache with the SLRU policy has two queues: the probationary one is colder than the protected one. A cache with `use_split_cache` has a data and a system queue: the system one, which holds the metadata needed to read the data, is more important than data one, so 'hottest' walks it first and 'coldest' last. The queue of each segment is in the `slru_queue` and `split_queue` columns; a split cache with the SLRU policy fills both.

Caches with the `LRU_OVERCOMMIT` and `SLRU_OVERCOMMIT` policies are not supported.

The walk first takes at most `max_elements` entries of the queue (for 'random', the sample), and only then reads their segments, without blocking the queue. The result is not an atomic snapshot: a segment which is removed from the cache in the meantime is skipped, so the result can have fewer than `max_elements` segments even if the cache holds more. When a segment's cache key is locked by another query (e.g. while the segment is being downloaded), the query waits for it.

`max_elements` can be at most the `max_observed_elements` setting of the cache (1000000 by default), unless the cache holds fewer segments. Since 'random' walks the whole queue, it also requires that the cache holds at most `max_observed_elements` segments, however small `max_elements` is.

Unlike `system.filesystem_cache`, which walks all keys of all caches, it walks the eviction queue of one cache, and with 'hottest' and 'coldest' only as far as needed, so it can return the hottest segments (e.g. to prewarm another replica), the next eviction candidates, or a uniform sample of the cache.

It requires the `SELECT` privilege on `system.filesystem_cache`.)",
            .syntax = "filesystemCache(cache_name, max_elements, max_total_size[, order])",
            .arguments = {
                {"cache_name", "The name of the cache, the same as in `SYSTEM DROP FILESYSTEM CACHE` and `system.filesystem_cache.cache_name`.", {"String"}},
                {"max_elements", "The maximum number of returned segments. At most the `max_observed_elements` setting of the cache, unless the cache holds fewer segments.", {"UInt64"}},
                {"max_total_size", "The maximum sum of `size` of the returned segments.", {"UInt64"}},
                {"order", "'hottest' (the default): the highest-priority segments first. 'coldest': the next eviction candidates first. 'random': a uniform sample of segments, in random order.", {"String"}},
            },
            .returned_value = {"A table with the columns `priority_rank`, `path`, `key`, `offset`, `size`, `state`, `split_queue`, `slru_queue` and `user_id`, one row per segment.", {}},
            .examples = {
                {"The paths of the hottest segments, up to 10000 of them and 10 GiB in total", "SELECT path FROM filesystemCache('s3_cache', 10000, 10737418240);", ""},
                {"The next eviction candidates", "SELECT priority_rank, path, size, slru_queue FROM filesystemCache('s3_cache', 100, 1073741824, 'coldest');", ""},
                {"A uniform sample of the cache", "SELECT path FROM filesystemCache('s3_cache', 1000, 10737418240, 'random');", ""},
            },
            .introduced_in = {26, 12},
            .category = FunctionDocumentation::Category::TableFunction,
        },
        SecretArgumentsSpec{},
        {.allow_readonly = true});
}

}
