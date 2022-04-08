#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>

#include <cctype>

namespace DB 
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class AggregateFunctionOrderBy final : IAggregateFunctionHelper<AggregateFunctionOrderBy> {
private:
	AggregateFunctionPtr nested_func;
	size_t num_arguments;
    mutable std::vector<MutableColumnPtr> data;
    size_t number_of_arguments_for_sorting;
    mutable std::vector<bool> bitmap;

    void parseString(String& s)
    {
        bool flag = false;
        for (auto symb : s) {
            if (flag) {
                if (symb == 'A') {
                    bitmap.push_back(true);
                } else {
                    bitmap.push_back(false);
                }
            }
            if (symb == ',')
                if (!flag) {
                    bitmap.push_back(true);
                    flag = !flag;
                }
            if (isalpha(symb))
                continue;
            if (isspace(symb)) {
                flag = !flag;
            }
        }
        number_of_arguments_for_sorting = bitmap.size();
    }

    std::vector<std::pair<size_t, size_t>> findEqualRanges(size_t curr_idx) { 
        std::vector<std::pair<size_t, size_t>> ans;
        ans.emplace_back(0, data[0]->size()); 
        for (size_t i = 0; i != curr_idx; ++i) {
            if (ans.empty()) {
                return ans;
            }
            std::vector<std::pair<size_t, size_t>> tmp;
            while (!ans.empty()) {
                auto range = ans.back(); 
                ans.pop_back();
                flag = false;
                size_t start;
                size_t counter = 0;
                Field prev = *(data[i])[range.first];
                Field curr;
                for (size_t j = range.first + 1, j <= range.second; ++j) {
                    if (data[i]->get(j, curr) == prev) {
                        counter += 1;
                        if (!flag) {
                            flag = true;
                            start = j - 1;
                        }
                    } else {
                        if (flag) {
                            flag = false;
                            tmp.emplace_back(start, start + counter);
                            counter = 0;
                        }
                    }
                }
            }
            ans = tmp;
        }
        return ans;
    }

public:
	AggregateFunctionOrderBy(AggregateFunctionPtr nested, const DataTypes & types, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionOrderBy>(types, params_)
        , nested_func(nested), num_arguments(types.size())
    {
        if (params_.size() == 0)
            throw Exception("Aggregate function " + getName() + " require at least one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (params_[0].getType() != Field::Types::Which::String)
            throw Exception("First parameter for aggregate function " + getName() + " must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        parseString(params_[0].get());
        if (num_arguments < number_of_arguments_for_sorting)
            throw Exception("Aggregate function " + getName() + " require at several arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

	String getName() const override
    {
        return nested_func->getName() + "OrderBy";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    void create(AggregateDataPtr place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr place) const noexcept override {
        nested_func->destroy(place);
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    bool hasTrivialDestructor() const override {
        return nested_func->hasTrivialDestructor();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override 
    {
        for (size_t i = 0; i != num_arguments; ++i) {
            if (data.size() <= i) {
                data.push_back((*columns)[i].cloneEmpty());
            }
            data[i]->insertFrom((*columns)[i], row_num);
        }               
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override 
    {
        bool is_first_column_for_sorting = true;
        size_t j = 0; 
        for (size_t i = num_arguments - number_of_arguments_for_sorting; i != num_arguments; ++i) {
            Permutation permutation;
            for (size_t i = 0; i != data[0]->size(); ++i) {
                permutation.push_back(i);
            }
            
            IColumn::PermutationSortDirection directon;
            if (bitmap[j])
                directon = IColumn::PermutationSortDirection::Ascending
            else
                directon = IColumn::PermutationSortDirection::Descending
            
            if (!is_first_column_for_sorting) {
                std::vector<std::pair<size_t, size_t>> ranges = findEqualRanges(i);
                data[i]->updatePermutation(directon, IColumn::PermutationStability::Stable, 0, 1, permutation, ranges);
            } else {
                data[i]->getPermutation(directon, IColumn::PermutationStability::Stable, 0, 1, permutation);
                is_first_column_for_sorting = false;
            }

            for (size_t h = num_arguments - number_of_arguments_for_sorting; i != num_arguments; ++i)
                data[h] = IColumn::mutate(data[h]->permute(permutation, 0));
            j += 1
        }

        IColumn * columns[num_arguments - number_of_arguments_for_sorting];
        for (size_t i = 0; i != num_arguments - number_of_arguments_for_sorting; ++i) {
            columns[i] = data[i].get();
        } 

        for (size_t i = 0; i != data[0].size(); ++i) {
            nested_func->add(place, columns, i, arena);
        }

        nested_func->insertResultInto(place, to, arena);
    }
};
}