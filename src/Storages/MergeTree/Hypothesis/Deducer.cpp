#include "Deducer.hpp"
#include <iterator>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <base/StringRef.h>
#include <Poco/Exception.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Columns/IColumn.h"
#include "Columns/IColumn_fwd.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Interpreters/Context.h"
#include "Interpreters/InterpreterSelectQuery.h"
#include "Parsers/ParserQuery.h"
#include "Parsers/TokenIterator.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include "Processors/Sources/SourceFromSingleChunk.h"
#include "QueryPipeline/Pipe.h"
#include "QueryPipeline/QueryPipelineBuilder.h"
#include "Storages/MergeTree/Hypothesis/Token.hpp"

namespace
{

const std::vector<std::string> transformers = {"identity", "base64Encode", "lower", "upper", "trimBoth"};

using namespace DB::Hypothesis;
class ColumnEntries
{
public:
    /*
            <length_of_string, index_of_entry_column>
        */
    struct ColumnEntry
    {
        size_t length;
        size_t col_idx;
        size_t transformer_idx;
    };

    ColumnEntries() = default;

    void addEntry(ColumnEntry entry, size_t entry_index)
    {
        if (entries.size() <= entry_index)
        {
            entries.resize(entry_index + 1);
        }
        entries[entry_index].push_back(entry);
    }

    void sortEntries()
    {
        for (auto & entry : entries)
        {
            std::sort(entry.begin(), entry.end(), [](const auto & lhs, const auto & rhs) { return lhs.col_idx < rhs.col_idx; });
        }
    }

    size_t size() const { return entries.size(); }

    const std::vector<ColumnEntry> & at(size_t idx) const { return entries[idx]; }

private:
    std::vector<std::vector<ColumnEntry>> entries;
};

DB::Block executeQuery(std::string_view query_text, const DB::Block & input, DB::ContextPtr context)
{
    DB::Tokens tokens(query_text.data(), query_text.data() + query_text.size());
    DB::IParser::Pos token_iterator(tokens, 100, 100);
    DB::ParserQuery parser(query_text.data() + query_text.size(), false, true);
    DB::ASTPtr query;
    DB::Expected expected;
    if (!parser.parse(token_iterator, query, expected))
    {
        throw Poco::Exception(fmt::format("Bad query: {}", query_text));
    }
    DB::Block block(input.cloneWithoutColumns());
    block.setColumns(input.getColumns());
    DB::Pipe pipe(std::make_shared<DB::SourceFromSingleChunk>(std::move(block)));
    DB::InterpreterSelectQuery select(query, context, std::move(pipe), DB::SelectQueryOptions());
    auto pipeline_builder = select.buildQueryPipeline();
    auto pipeline = DB::QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    DB::PullingPipelineExecutor executor(pipeline);
    DB::Block res;
    if (!executor.pull(res))
    {
        throw Poco::Exception("Hypothesis pipeline is broken");
    }
    if (executor.pull(res))
    {
        throw Poco::Exception("More data is not expected");
    }
    return res;
}

void fillColumnsEntries(const DB::Block & block, size_t deduce_col_idx, std::vector<ColumnEntries> & column_entries_vec, LoggerPtr)
{
    auto deduce_col_name = block.getByPosition(deduce_col_idx).name;
    DB::ContextPtr context = DB::Context::getGlobalContextInstance();
    // TODO(azakarka) aho corasick
    for (size_t col = 0; col < block.columns(); ++col)
    {
        if (col == deduce_col_idx)
        {
            continue;
        }
        auto col_name = block.getByPosition(col).name;

        // multisearch stage
        std::string multisearch_query;
        for (size_t i = 0; i < transformers.size(); ++i)
        {
            multisearch_query += fmt::format("{}({})", transformers[i], block.getByPosition(col).name);
            if (i + 1 != transformers.size())
                multisearch_query += ", ";
        }
        multisearch_query = fmt::format("multiSearchAllPositions({}, [{}]) as __inds", deduce_col_name, multisearch_query);
        auto multisearch_res = executeQuery(multisearch_query, block, context);
        for (size_t transformer_idx = 0; transformer_idx < transformers.size(); ++transformer_idx)
        {
            // collect indexes

            DB::ColumnWithTypeAndName occurence_col;
            {
                auto tmp_res
                    = executeQuery(fmt::format("arrayElement(__inds, {}) as __idx", transformer_idx + 1), multisearch_res, context);
                occurence_col = tmp_res.getByPosition(0);
            }
            // check if all have occurence
            {
                auto tmp_res = executeQuery("sum(__idx = 0) > 0 as __should_skip", DB::Block({occurence_col}), context);
                if (tmp_res.getByName("__should_skip").column->getBool(0))
                {
                    continue;
                }
            }
            DB::ColumnPtr length_col;
            {
                auto tmp_res = executeQuery(fmt::format("length({}({}))", transformers[transformer_idx], col_name), block, context);
                length_col = tmp_res.getByPosition(0).column;
            }
            for (size_t row = 0; row < block.rows(); ++row)
            {
                auto occurence_idx = occurence_col.column->getUInt(row);
                auto len = length_col->getUInt(row);
                while (true)
                {
                    column_entries_vec[row].addEntry(
                        ColumnEntries::ColumnEntry{.length = len, .col_idx = col, .transformer_idx = transformer_idx}, occurence_idx - 1);
                    auto transformer_expr = fmt::format("{}({})", transformers[transformer_idx], col_name);
                    auto res = executeQuery(
                        fmt::format(
                            "position({}, {}, {}), length({})", deduce_col_name, transformer_expr, occurence_idx + 1, transformer_expr),
                        block,
                        context);
                    // TODO fixme later:
                    if (auto new_pos = res.getByPosition(0).column->getUInt(row); new_pos == 0)
                    {
                        break;
                    }
                    else
                    {
                        occurence_idx = new_pos;
                        len = res.getByPosition(1).column->getUInt(row);
                    }
                }
            }
        }
    }

    for (size_t row = 0; row < block.rows(); ++row)
    {
        column_entries_vec[row].sortEntries();
    }
}

size_t calculateLongestCommonSubstr(std::vector<size_t> & indices, DB::ColumnPtr col)
{
    for (size_t common_substr_sz = 0; common_substr_sz <= col->getDataAt(0).size; ++common_substr_sz)
    {
        if (indices[0] + common_substr_sz == col->getDataAt(0).size)
        {
            return common_substr_sz;
        }
        char common_char = col->getDataAt(0).data[indices[0] + common_substr_sz];
        for (size_t row = 0; row < col->size(); ++row)
        {
            if (indices[row] + common_substr_sz == col->getDataAt(row).size)
            {
                return common_substr_sz;
            }
            if (col->getDataAt(row).data[indices[row] + common_substr_sz] != common_char)
            {
                return common_substr_sz;
            }
        }
    }
    return col->getDataAt(0).size;
}

void traverseColumnEntriesImpl(
    std::vector<size_t> & indices,
    HypothesisBuilder state_builder,
    DB::ColumnPtr deduction_col,
    const std::vector<ColumnEntries> & columns_entries,
    HypothesisList & hypothesis_list,
    TransformerToken::IndexMapper index_mapper,
    LoggerPtr log)
{
    size_t rows_cnt = deduction_col->size();
    {
        bool finished = true;
        for (size_t i = 0; i < rows_cnt; ++i)
        {
            finished &= deduction_col->getDataAt(i).size == indices[i];
        }
        if (finished)
        {
            hypothesis_list.push_back(std::move(state_builder).constuctHypothesis());
            return;
        }
    }
    auto new_indices = indices;
    if (columns_entries[0].size() > indices[0] && !columns_entries[0].at(indices[0]).empty())
    {
        std::vector<size_t> entry_indices(rows_cnt, 0);
        size_t current_max_col_idx = 0;
        size_t transformer_idx = 0;
        bool finished = false;
        // All column_entries are sorted
        // So each iteration we find minimum and check if all columns has this entry
        // Then proceed to next minimum
        while (!finished)
        {
            bool found_common = true;
            for (size_t row = 0; row < rows_cnt; ++row)
            {
                const auto & entries = columns_entries[row].at(indices[row]);
                while (entry_indices[row] < entries.size() && entries[entry_indices[row]].col_idx < current_max_col_idx)
                {
                    ++entry_indices[row];
                }
                if (entry_indices[row] == entries.size())
                {
                    found_common = false;
                    finished = true;
                    break;
                }
                if (entries[entry_indices[row]].col_idx != current_max_col_idx)
                {
                    found_common = false;
                    current_max_col_idx = entries[entry_indices[row]].col_idx;
                    transformer_idx = entries[entry_indices[row]].transformer_idx;
                    break;
                }
                else
                {
                    transformer_idx = entries[entry_indices[row]].transformer_idx;
                }
            }
            if (found_common)
            {
                auto derived = state_builder;
                derived.addToken(createTransformerToken(transformers[transformer_idx], current_max_col_idx, index_mapper));
                for (size_t row = 0; row < rows_cnt; ++row)
                {
                    const auto & row_entries = columns_entries[row].at(indices[row]);
                    new_indices[row] = indices[row] + row_entries[entry_indices[row]].length;
                }
                traverseColumnEntriesImpl(new_indices, derived, deduction_col, columns_entries, hypothesis_list, index_mapper, log);
                ++current_max_col_idx;
            }
        }
    }
    if (state_builder.empty() || state_builder.back()->getType() != TokenType::Const)
    {
        size_t common_substr_len = calculateLongestCommonSubstr(indices, deduction_col);
        for (size_t len = 1; len <= common_substr_len; ++len)
        {
            auto derived = state_builder;
            std::string_view const_value = deduction_col->getDataAt(0).toView().substr(indices[0], len);
            derived.addToken(createConstToken(std::string(const_value)));

            for (size_t row = 0; row < rows_cnt; ++row)
            {
                new_indices[row] = indices[row] + len;
            }
            traverseColumnEntriesImpl(new_indices, derived, deduction_col, columns_entries, hypothesis_list, index_mapper, log);
        }
    }
}

void traverseColumnEntries(
    const std::vector<ColumnEntries> & columns_entries,
    DB::ColumnPtr deduction_col,
    HypothesisList & hypothesis_list,
    TransformerToken::IndexMapper index_mapper,
    std::string_view col_to_deduce_name,
    LoggerPtr log)
{
    size_t rows_cnt = deduction_col->size();
    assert(columns_entries.size() == rows_cnt);

    std::vector<size_t> indices(rows_cnt, 0);
    auto builder = HypothesisBuilder();
    builder.setTargetColumn(col_to_deduce_name);
    traverseColumnEntriesImpl(indices, builder, deduction_col, columns_entries, hypothesis_list, index_mapper, log);
}

}


namespace DB::Hypothesis
{

Deducer::Deducer(Block block_)
    : block(std::move(block_))
{
    if (block.rows() == 0 || block.columns() == 0)
    {
        throw Poco::Exception("Invalid input block");
    }
    log = getLogger("HypothesisDeducer");
    std::vector<size_t> inds_to_delete;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (block.getDataTypes()[i]->getTypeId() != DB::TypeIndex::String)
        {
            inds_to_delete.push_back(i);
        }
    }
    std::reverse(inds_to_delete.begin(), inds_to_delete.end());
    for (const size_t idx : inds_to_delete)
    {
        block.erase(idx);
    }
}

HypothesisList Deducer::deduceColumn(std::string_view name)
{
    int col_idx = -1;
    auto index_mapper = block.getNames();
    for (int i = 0; i < std::ssize(index_mapper); ++i)
    {
        if (index_mapper[i] == name)
        {
            col_idx = i;
        }
    }
    if (col_idx == -1)
    {
        LOG_WARNING(log, "Column {} was not found in hypothesis block", name);
        return {};
    }
    size_t rows_cnt = block.rows();
    std::vector<ColumnEntries> column_entries_vec(rows_cnt);
    fillColumnsEntries(block, col_idx, column_entries_vec, log);
    HypothesisList hypothesis_list;
    traverseColumnEntries(
        column_entries_vec,
        block.getByPosition(col_idx).column,
        hypothesis_list,
        std::make_shared<const std::vector<std::string>>(std::move(index_mapper)),
        name,
        log);
    LOG_DEBUG(log, "Deduced {} hypothesis for {}", hypothesis_list.size(), name);
    return hypothesis_list;
}

}
