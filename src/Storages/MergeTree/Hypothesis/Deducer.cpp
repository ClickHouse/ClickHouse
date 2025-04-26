#include "Deducer.hpp"
#include <iterator>

#include <Core/NamesAndTypes.h>
#include <base/StringRef.h>
#include <Poco/Exception.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Columns/IColumn.h"
#include "Core/Block.h"
#include "Storages/MergeTree/Hypothesis/Token.hpp"

namespace
{
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

void fillColumnsEntries(const DB::Block & block, size_t col_idx, size_t row_idx, ColumnEntries & column_entries, LoggerPtr)
{
    // TODO(azakarka) aho corasick
    auto get_ref = [&block, row_idx](size_t idx) { return block.getByPosition(idx).column->getDataAt(row_idx); };
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (i == col_idx)
        {
            continue;
        }
        size_t n = get_ref(col_idx).size;
        size_t m = get_ref(i).size;
        if (n <= m)
        {
            continue;
        }
        for (size_t j = 0; j <= n - m; ++j)
        {
            if (get_ref(col_idx).toView().substr(j, m) == get_ref(i).toView())
            {
                column_entries.addEntry(ColumnEntries::ColumnEntry{.length = m, .col_idx = i}, j);
            }
        }
    }
    column_entries.sortEntries();
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
    IdentityToken::IndexMapper index_mapper,
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
            hypothesis_list.emplace_back(std::move(state_builder).constuctHypothesis());
            return;
        }
    }
    auto new_indices = indices;
    if (columns_entries[0].size() > indices[0] && !columns_entries[0].at(indices[0]).empty())
    {
        std::vector<size_t> entry_indices(rows_cnt, 0);
        size_t current_max_col_idx = 0;
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
                    break;
                }
            }
            if (found_common)
            {
                auto derived = state_builder;
                derived.addToken(createIdentityToken(current_max_col_idx, index_mapper));
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
    IdentityToken::IndexMapper index_mapper,
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
            inds_to_delete.emplace_back(i);
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
    std::vector<ColumnEntries> columns_entries(rows_cnt);
    for (size_t i = 0; i < rows_cnt; ++i)
    {
        fillColumnsEntries(block, col_idx, i, columns_entries[i], log);
    }
    HypothesisList hypothesis_list;
    traverseColumnEntries(
        columns_entries,
        block.getByPosition(col_idx).column,
        hypothesis_list,
        std::make_shared<const std::vector<std::string>>(std::move(index_mapper)),
        name,
        log);
    LOG_DEBUG(log, "Deduced {} hypothesis for {}", hypothesis_list.size(), name);
    return hypothesis_list;
}

}
