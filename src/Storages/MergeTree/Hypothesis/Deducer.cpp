#include "Deducer.hpp"
#include <iterator>
#include <string_view>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <base/StringRef.h>
#include <Poco/Exception.h>
#include "Common/Logger.h"
#include "Common/Volnitsky.h"
#include "Common/logger_useful.h"
#include "Columns/ColumnArray.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Columns/IColumn.h"
#include "Columns/IColumn_fwd.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"
#include "Functions/FunctionFactory.h"
#include "Interpreters/Context.h"
#include "Interpreters/InterpreterSelectQuery.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
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

        bool operator<(const ColumnEntry & other) const
        {
            return std::tie(col_idx, transformer_idx) < std::tie(other.col_idx, other.transformer_idx);
        }

        bool operator==(const ColumnEntry & other) const
        {
            return std::tie(col_idx, transformer_idx) == std::tie(other.col_idx, other.transformer_idx);
        }
        bool operator!=(const ColumnEntry & other) const { return !this->operator==(other); }
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
            std::sort(entry.begin(), entry.end());
        }
    }

    size_t size() const { return entries.size(); }

    const std::vector<ColumnEntry> & at(size_t idx) const { return entries[idx]; }

private:
    std::vector<std::vector<ColumnEntry>> entries;
};

std::vector<DB::ColumnPtr> transformBlock(
    const DB::Block & block,
    const std::vector<std::pair<size_t, size_t>> col_transformer_pairs,
    const std::vector<DB::FunctionBasePtr> & transformers_fn,
    size_t rows)
{
    auto data_type_string = std::make_shared<DB::DataTypeString>();
    std::vector<DB::ColumnPtr> result;
    for (const auto & [col, transformer_idx] : col_transformer_pairs)
    {
        DB::ColumnsWithTypeAndName arguments{block.getByPosition(col).cloneEmpty()};
        arguments.front().column = block.getByPosition(col).column->cut(0, rows);

        auto res = transformers_fn[transformer_idx]->execute(arguments, data_type_string, rows, false);
        result.push_back(res);
    }
    return result;
}

void fillColumnsEntries(const DB::Block & block, size_t deduce_col_idx, std::vector<ColumnEntries> & column_entries_vec, LoggerPtr)
{
    DB::ContextPtr context = DB::Context::getGlobalContextInstance();
    // input: n columns
    // output: nxm columns of transformers
    auto data_type_string = std::make_shared<DB::DataTypeString>();
    auto data_type_array_of_strings = std::make_shared<DB::DataTypeArray>(data_type_string);
    auto data_type_array_of_ints = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeUInt32>());
    auto & function_factory = DB::FunctionFactory::instance();

    std::vector<DB::FunctionBasePtr> transformers_fn;
    {
        DB::ColumnsWithTypeAndName arguments{DB::ColumnWithTypeAndName(data_type_string, /*name_=*/"test")};
        for (const auto & transformer : transformers)
        {
            transformers_fn.push_back(function_factory.get(transformer, context)->build(arguments));
        }
    }
    std::vector<std::pair<size_t, size_t>> permited_pairs;
    for (size_t col = 0; col < block.columns(); ++col)
    {
        if (col == deduce_col_idx)
        {
            continue;
        }
        for (size_t transformer_idx = 0; transformer_idx < transformers.size(); ++transformer_idx)
        {
            permited_pairs.emplace_back(col, transformer_idx);
        }
    }
    {
        std::vector<DB::ColumnPtr> all_transformed = transformBlock(block, permited_pairs, transformers_fn, 1);
        auto needles_col = DB::ColumnString::create();
        for (const auto & column : all_transformed)
        {
            auto str = column->getDataAt(0);
            needles_col->insertData(str.data, str.size);
        }
        auto offsets_col = DB::ColumnVector<DB::ColumnArray::Offset>::create();
        offsets_col->insertValue(all_transformed.size());
        auto needles_arr = DB::ColumnArray::create(std::move(needles_col), std::move(offsets_col));
        DB::ColumnsWithTypeAndName multi_search_args{
            block.getByPosition(deduce_col_idx).cloneEmpty(), DB::ColumnWithTypeAndName(data_type_array_of_strings, "needles")};
        multi_search_args.front().column = block.getByPosition(deduce_col_idx).column->cut(0, 1);
        multi_search_args.back().column = needles_arr->getPtr();

        auto multi_search_fn = function_factory.get("multiSearchAllPositions", context)->build(multi_search_args);
        auto search_res = multi_search_fn->execute(multi_search_args, data_type_array_of_ints, 1, false);
        const DB::ColumnArray * array_col = typeid_cast<const DB::ColumnArray *>(search_res.get());
        const DB::ColumnVector<UInt64> * data_col = typeid_cast<const DB::ColumnVector<UInt64> *>(&array_col->getData());
        std::vector<std::pair<size_t, size_t>> new_permited_pairs;
        for (size_t i = 0; i < permited_pairs.size(); ++i)
        {
            size_t occurence_idx = data_col->getUInt(i);
            if (occurence_idx != 0)
            {
                new_permited_pairs.push_back(permited_pairs[i]);
            }
        }
        permited_pairs = std::move(new_permited_pairs);
    }
    auto transformed = transformBlock(block, permited_pairs, transformers_fn, block.rows());
    for (size_t i = 0; i < permited_pairs.size(); ++i)
    {
        const auto [col, transformer_idx] = permited_pairs[i];
        for (size_t row = 0; row < block.rows(); ++row)
        {
            std::string_view haystack = block.getByPosition(deduce_col_idx).column->getDataAt(row).toView();
            std::string_view needle = transformed[i]->getDataAt(row).toView();
            DB::Volnitsky searcher(needle.data(), needle.size());
            size_t last_occurence = 0;
            while (true)
            {
                const char * stop = searcher.search(haystack.data() + last_occurence, haystack.size() - last_occurence);
                last_occurence = stop - haystack.data();
                if (last_occurence == haystack.size())
                {
                    break;
                }
                column_entries_vec[row].addEntry(
                    ColumnEntries::ColumnEntry{.length = needle.size(), .col_idx = col, .transformer_idx = transformer_idx},
                    last_occurence);
                ++last_occurence;
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
        size_t finished = 0;
        for (size_t i = 0; i < rows_cnt; ++i)
        {
            finished += deduction_col->getDataAt(i).size == indices[i];
        }
        if (finished == rows_cnt)
        {
            hypothesis_list.push_back(std::move(state_builder).constuctHypothesis());
        }
        if (finished > 0)
        {
            return;
        }
    }
    auto new_indices = indices;

    bool has_any_entries_in_all_rows = true;
    for (size_t i = 0; i < rows_cnt; ++i)
    {
        has_any_entries_in_all_rows &= indices[i] < columns_entries[i].size() && !columns_entries[i].at(indices[i]).empty();
    }
    if (has_any_entries_in_all_rows)
    {
        auto get_nth_array = [&columns_entries, &indices](size_t idx) { return &columns_entries[idx].at(indices[idx]); };
        std::vector<size_t> entry_indices(rows_cnt, 0);
        auto common_entry = get_nth_array(0)->at(0);
        bool finished = false;
        // All column_entries are sorted
        // So each iteration we find minimum and check if all columns has this entry
        // Then proceed to next minimum
        while (!finished)
        {
            bool found_common = true;
            for (size_t row = 0; row < rows_cnt; ++row)
            {
                const auto & entries = *get_nth_array(row);
                while (entry_indices[row] < entries.size() && entries[entry_indices[row]] < common_entry)
                {
                    ++entry_indices[row];
                }
                if (entry_indices[row] == entries.size())
                {
                    found_common = false;
                    finished = true;
                    break;
                }
                if (entries[entry_indices[row]] != common_entry)
                {
                    found_common = false;
                    common_entry = entries[entry_indices[row]];
                    break;
                }
            }
            if (found_common)
            {
                auto derived = state_builder;
                derived.addToken(createTransformerToken(transformers[common_entry.transformer_idx], common_entry.col_idx, index_mapper));
                for (size_t row = 0; row < rows_cnt; ++row)
                {
                    const auto & row_entries = columns_entries[row].at(indices[row]);
                    new_indices[row] = indices[row] + row_entries[entry_indices[row]].length;
                }
                traverseColumnEntriesImpl(new_indices, derived, deduction_col, columns_entries, hypothesis_list, index_mapper, log);

                if (entry_indices[0] + 1 == get_nth_array(0)->size())
                {
                    finished = true;
                    break;
                }
                common_entry = get_nth_array(0)->at(entry_indices[0] + 1);
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
