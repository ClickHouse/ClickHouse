#include "PartitionOutputFormat.h"

#include <Columns/IColumn.h>
#include <Common/ArenaUtils.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/map.hpp>

#include <ranges>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

const std::string PARTITION_ID_WILDCARD = "_partition_id";

String formatTemplate(const String & out_file_template, const PartitionOutputFormat::Key & key, const absl::flat_hash_map<StringRef, size_t>& key_name_to_index)
{
    String res;
    std::vector<bool> used(key.size());
    size_t n = out_file_template.size();
    std::vector<std::string> columns(
        begin(std::views::keys(key_name_to_index)),
        end(std::views::keys(key_name_to_index)));

    for (size_t i = 0; i < n; ++i)
    {
        char x = out_file_template[i];

        if (x == '\\' && i + 1 < n)
        {
            if (out_file_template[i + 1] == '{')
            {
                ++i;
                res.push_back('{');
                continue;
            }
            if (out_file_template[i + 1] == '}')
            {
                ++i;
                res.push_back('}');
                continue;
            }
        }
        if (x == '}')
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not escaped '}}' in out_file_template pattern at pos {}. Escape it using backslash '\'", i);
        }
        if (x != '{')
        {
            res.push_back(x);
            continue;
        }

        std::string name;
        bool found = false;
        for (size_t j = i + 1; j < n; ++j)
        {
            x = out_file_template[j];
            if (x == '\\' && j + 1 < n)
            {
                if (out_file_template[j + 1] == '{')
                {
                    ++j;
                    name.push_back('{');
                    continue;
                }
                if (out_file_template[j + 1] == '}')
                {
                    ++j;
                    name.push_back('}');
                    continue;
                }
            }
            if (x == '{')
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not escaped '{{' inside out_file_template pattern at pos {}. Escape it using backslash '\'", j);
            }
            if (x != '}')
            {
                name.push_back(x);
                continue;
            }
            found = true;
            auto it = key_name_to_index.find(name);
            size_t key_index = 0;
            if (it != key_name_to_index.end()) key_index = it->second;
            else
            {
                if (name != PARTITION_ID_WILDCARD)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column name in out_file_template: {}. Available columns: {}.", name, boost::algorithm::join(columns, ", "));
                if (key.size() != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column name in out_file_template: {}. Can only use {{_partition_id}} with one key. Available columns: {}.", name, boost::algorithm::join(columns, ", "));
            }

            used[key_index] = true;
            res.append(key[key_index].toView());
            i = j;
            name.clear();
            break;
        }
        if (!found)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No matching '}}' for '{{' in out_file_template at pos {}", i);
        }
    }
    if (!std::all_of(begin(used), end(used), std::identity{}))
    {
        auto missed_columns_view = key_name_to_index
            | std::views::filter([&](const auto & pair) { return !used[pair.second];})
            | std::views::keys;
        std::vector<std::string> missed_columns(missed_columns_view.begin(), missed_columns_view.end());
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missed columns in out_file_template: {}. Must use all of them", boost::algorithm::join(missed_columns, ", "));
    }
    return res;
}

const ASTs & getChildrensInPartitionBy(const ASTPtr & expr_list)
{
    if (expr_list->children.size() != 1)
    {
        return expr_list->children;
    }
    const auto * func = expr_list->children.front()->as<ASTFunction>();
    if (!func || func->name != "tuple")
    {
        return expr_list->children;
    }
    return func->arguments->children;
}

PartitionOutputFormat::PartitionOutputFormat(
    const OutputFormatForPath & output_format_for_path_,
    DynamicWriteBufferManager & write_buffers_manager_,
    const Block & header_,
    const String & out_file_template_,
    const ASTPtr & partition_by,
    const ContextPtr & context)
    : IOutputFormat(header_, write_buffers_manager_), header(header_), out_file_template(out_file_template_), output_format_for_path(output_format_for_path_), write_buffers_manager(write_buffers_manager_)
{
    int i = 0;
    for (const ASTPtr & expr : getChildrensInPartitionBy(partition_by))
    {
        auto column = copyStringInArena(partition_keys_arena, expr->getAliasOrColumnName());
        partition_key_name_to_index.emplace(column, i++);

        ASTs arguments(1, expr);
        ASTPtr partition_by_string = makeASTFunction("toString", std::move(arguments));
        partition_by_expr_names.push_back(partition_by_string->getColumnName());

        auto syntax_result = TreeRewriter(context).analyze(partition_by_string, header.getNamesAndTypesList());
        partition_by_exprs.push_back(ExpressionAnalyzer(partition_by_string, syntax_result, context).getActions(false));
    }
}

void PartitionOutputFormat::consume(Chunk chunk)
{
    Columns key_columns;
    const auto & columns = chunk.getColumns();
    for (size_t i = 0; i < partition_by_exprs.size(); ++i)
    {
        Block block_with_partition_by_expr = header.cloneWithoutColumns();
        block_with_partition_by_expr.setColumns(columns);
        partition_by_exprs[i]->execute(block_with_partition_by_expr);
        key_columns.push_back(std::move(block_with_partition_by_expr.getByName(partition_by_expr_names[i]).column));
    }

    absl::flat_hash_map<Key, size_t, KeyHash> key_to_chunk_index;
    IColumn::Selector selector;
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        Key key;
        key.reserve(key_columns.size());
        for (auto & key_column : key_columns)
        {
            key.push_back(key_column->getDataAt(row));
        }
        auto [it, _] = key_to_chunk_index.emplace(key, key_to_chunk_index.size());
        selector.push_back(it->second);
    }

    Chunks sub_chunks;
    sub_chunks.reserve(key_to_chunk_index.size());
    for (size_t column_index = 0; column_index < columns.size(); ++column_index)
    {
        MutableColumns column_sub_chunks = columns[column_index]->scatter(key_to_chunk_index.size(), selector);
        if (column_index == 0) /// Set sizes for sub-chunks.
        {
            for (const auto & column_sub_chunk : column_sub_chunks)
            {
                sub_chunks.emplace_back(Columns(), column_sub_chunk->size());
            }
        }
        for (size_t sub_chunk_index = 0; sub_chunk_index < column_sub_chunks.size(); ++sub_chunk_index)
        {
            sub_chunks[sub_chunk_index].addColumn(std::move(column_sub_chunks[sub_chunk_index]));
        }
    }

    for (const auto & [partition_key, index] : key_to_chunk_index)
    {
        getOrCreateOutputFormat(partition_key)->write(header.cloneWithColumns(sub_chunks[index].detachColumns()));
    }
}

OutputFormatPtr PartitionOutputFormat::getOrCreateOutputFormat(const Key & key)
{
    auto it = partition_key_to_output_format.find(key);
    if (it == partition_key_to_output_format.end())
    {
        auto filepath = formatTemplate(out_file_template, key, partition_key_name_to_index);
        auto output_format = output_format_for_path(filepath);
        std::tie(it, std::ignore) = partition_key_to_output_format.emplace(copyStringsInArena(partition_keys_arena, key), output_format);
    }
    return it->second;
}

void PartitionOutputFormat::finalizeImpl()
{
    for (auto & [_, output_format] : partition_key_to_output_format)
    {
        output_format->finalize();
    }
}

void PartitionOutputFormat::flushImpl()
{
    write_buffers_manager.proxyNext();
}

void throwIfTemplateIsNotValid(const String & out_file_template, const ASTPtr & partition_by)
{
    absl::flat_hash_map<StringRef, size_t> partition_key_name_to_index;
    Arena arena;
    size_t i = 0;
    for (const ASTPtr & expr : getChildrensInPartitionBy(partition_by))
    {
        StringRef column = copyStringInArena(arena, expr->getAliasOrColumnName());
        partition_key_name_to_index.emplace(column, i++);
    }
    PartitionOutputFormat::Key dummy_key(partition_key_name_to_index.size());
    formatTemplate(out_file_template, dummy_key, partition_key_name_to_index);
}

std::vector<StringRef> copyStringsInArena(Arena & arena, const StringRefs & strings)
{
    std::vector<StringRef> res(strings.size());
    for (size_t i = 0; i < strings.size(); ++i)
    {
        res[i] = copyStringInArena(arena, strings[i]);
    }
    return res;
}

}
