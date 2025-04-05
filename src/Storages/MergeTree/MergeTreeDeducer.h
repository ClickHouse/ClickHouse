#pragma once
#include <string>
#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <base/StringRef.h>
#include "Common/Exception.h"

namespace DB
{


class MergeTreeDeducer
{
public:
    class Hypothesis
    {
    public:
        Hypothesis() = default;

        void addEntry(int index) { entries.push_back(index); }

        const std::vector<int> & getEntries() const { return entries; }

        std::string toString() const
        {
            std::string result;
            for (const auto & entry : entries)
            {
                result += "$" + std::to_string(entry);
            }
            return result;
        }

        std::string toStringWithNames(const NamesAndTypesList & names) const
        {
            std::string result;
            for (const auto & entry : entries)
            {
                if (entry >= static_cast<int>(names.size()))
                    result += "$" + std::to_string(entry);
                else
                    result += "$" + names.getNames()[entry];
            }
            return result;
        }

    private:
        std::vector<int> entries;
    };


    using HypothesisVector = std::vector<Hypothesis>;


    explicit MergeTreeDeducer(MergeTreeReaderPtr && reader_, int columns_cnt_)
        : reader(std::move(reader_))
        , columns_cnt(columns_cnt_)
    {
        if (columns_cnt_ < 0)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Columns count cannot be negative");
        }
    }

    using EntryIndex = std::pair<int, int>;
    using EntryIndices = std::vector<std::vector<EntryIndex>>;


    HypothesisVector deduce(int index)
    {
        if (index < 0 || index >= columns_cnt)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Deduction index should be in [0, column_cnt)");
        }
        auto col = getSample();
        EntryIndices entry_indices(col[index].size());
        fillEntryIndices(col, index, entry_indices);
        HypothesisVector hypothesis;
        traverseEntryIndices(entry_indices, hypothesis);
        return hypothesis;
    }

    bool check(Hypothesis hypothesis, int examined_index, int rows_cnt)
    {
        if (examined_index < 0 || examined_index >= columns_cnt)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Examined index should be in [0, column_cnt)");
        }
        Columns res_columns(columns_cnt);
        auto rows_count = reader->readRows(0, 0, false, rows_cnt, res_columns);
        for (int row_index = 0; row_index < static_cast<int>(rows_count); ++row_index)
        {
            std::vector<std::string> row;
            for (const auto & column : res_columns)
            {
                row.emplace_back() = column->getDataAt(row_index).toString();
            }
            const auto & entries = hypothesis.getEntries();
            std::string matching;
            for (const auto & entry : entries)
            {
                if (entry >= static_cast<int>(row.size()) || entry == examined_index)
                {
                    return false;
                }
                matching += row[entry];
            }
            if (examined_index >= static_cast<int>(row.size()) || matching != row[examined_index])
            {
                return false;
            }
        }
        return true;
    }

private:
    std::vector<std::string> getSample()
    {
        Columns res_columns(columns_cnt);
        if (reader->readRows(0, 0, false, 1, res_columns) < 1)
        {
            return {};
        }
        std::vector<std::string> sample_row;
        for (const auto & column : res_columns)
        {
            sample_row.emplace_back() = column->getDataAt(0).toString();
        }
        if (static_cast<int>(sample_row.size()) != columns_cnt)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "sample_row size and column_cnt mismatch");
        }
        return sample_row;
    }

    void fillEntryIndices(const std::vector<std::string> & col, int examined_index, EntryIndices & entry_indices)
    {
        // TODO aho corasick
        for (int i = 0; i < static_cast<int>(col.size()); ++i)
        {
            if (i == examined_index)
            {
                continue;
            }
            size_t n = col[examined_index].size();
            size_t m = col[i].size();
            if (n <= m)
            {
                return;
            }
            for (size_t j = 0; j <= n - m; ++j)
            {
                if (col[examined_index].substr(j, m) == col[i])
                {
                    entry_indices[j].emplace_back(static_cast<int>(m), i);
                }
            }
        }
    }

    void traverseEntryIndices(const EntryIndices & entryIndices, HypothesisVector & hypothesis)
    {
        traverseEntryIndicesImpl(0, Hypothesis(), entryIndices, hypothesis);
    }

    void traverseEntryIndicesImpl(int index, Hypothesis state, const EntryIndices & entry_indices, HypothesisVector & hypothesis)
    {
        if (index == static_cast<int>(entry_indices.size()))
        {
            hypothesis.emplace_back(state);
            return;
        }
        if (index > static_cast<int>(entry_indices.size()))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "index is higher than entry_index size");
        }
        for (const auto & [word_len, colIndex] : entry_indices[index])
        {
            Hypothesis state_derived = state;
            state_derived.addEntry(colIndex);
            traverseEntryIndicesImpl(index + word_len, state_derived, entry_indices, hypothesis);
        }
    }


    MergeTreeReaderPtr reader;
    int columns_cnt = 0;
};

}
