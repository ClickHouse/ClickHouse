#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/SelectQueryInfo.h>

#include <unordered_set>


namespace DB
{

class NamesAndTypesList;


namespace VirtualColumnUtils
{

/// Adds to the select query section `WITH value AS column_name`, and uses func
/// to wrap the value (if any)
///
/// For example:
/// - `WITH 9000 as _port`.
/// - `WITH toUInt16(9000) as _port`.
void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value, const String & func = "");

/// Prepare `expression_ast` to filter block. Returns true if `expression_ast` is not trimmed, that is,
/// `block` provides all needed columns for `expression_ast`, else return false.
bool prepareFilterBlockWithQuery(const ASTPtr & query, ContextPtr context, Block block, ASTPtr & expression_ast);

/// Leave in the block only the rows that fit under the WHERE clause and the PREWHERE clause of the query.
/// Only elements of the outer conjunction are considered, depending only on the columns present in the block.
/// If `expression_ast` is passed, use it to filter block.
void filterBlockWithQuery(const ASTPtr & query, Block & block, ContextPtr context, ASTPtr expression_ast = {});

/// Extract from the input stream a set of `name` column values
template <typename T>
auto extractSingleValueFromBlock(const Block & block, const String & name)
{
    std::unordered_set<T> res;
    const ColumnWithTypeAndName & data = block.getByName(name);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        res.insert((*data.column)[i].get<T>());
    return res;
}

NamesAndTypesList getPathAndFileVirtualsForStorage(NamesAndTypesList storage_columns);

ASTPtr createPathAndFileFilterAst(const ASTPtr & query, const NamesAndTypesList & virtual_columns, const String & path_example, const ContextPtr & context);

ColumnPtr getFilterByPathAndFileIndexes(const std::vector<String> & paths, const ASTPtr & query, const NamesAndTypesList & virtual_columns, const ContextPtr & context, ASTPtr filter_ast);

template <typename T>
void filterByPathOrFile(std::vector<T> & sources, const std::vector<String> & paths, const ASTPtr & query, const NamesAndTypesList & virtual_columns, const ContextPtr & context, ASTPtr filter_ast)
{
    auto indexes_column = getFilterByPathAndFileIndexes(paths, query, virtual_columns, context, filter_ast);
    const auto & indexes = typeid_cast<const ColumnUInt64 &>(*indexes_column).getData();
    if (indexes.size() == sources.size())
        return;

    std::vector<T> filtered_sources;
    filtered_sources.reserve(indexes.size());
    for (auto index : indexes)
        filtered_sources.emplace_back(std::move(sources[index]));
    sources = std::move(filtered_sources);
}

void addRequestedPathAndFileVirtualsToChunk(
    Chunk & chunk, const NamesAndTypesList & requested_virtual_columns, const String & path, const String * filename = nullptr);
}

}
