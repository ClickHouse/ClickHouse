#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/TypeId.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <cctype>
#include <memory>
#include <optional>
#include <type_traits>
#include <optional>

namespace DB 
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct OrderByData {
    std::vector<MutableColumnPtr> data;
    std::vector<bool> bitmap;
    size_t num_arguments;
    size_t num_arguments_for_sorting;

    void parseString(const String& s)
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
        num_arguments_for_sorting = bitmap.size();
    }

    OrderByData(const String& param_str, const DataTypes& types)   
    {
        num_arguments = types.size();
        parseString(param_str);
        data.reserve(bitmap.size());
        for (size_t i = 0; i != num_arguments; ++i) {
            data.push_back(types[i]->createColumn());
        }
    }

    std::unique_ptr<IColumn*[]> sort() {
        IColumn::Permutation p;
        for (size_t i = 0; i != data[0]->size(); ++i) {
            p.push_back(i);
        }

        std::unique_ptr<IColumn*[]> ptr{new IColumn*[num_arguments - num_arguments_for_sorting]}; 
        EqualRanges er(1, std::pair<size_t, size_t>(0, data[0]->size()));

        for (size_t i = num_arguments - num_arguments_for_sorting; i != num_arguments; ++i) {
            data[i]->updatePermutation(
                bitmap[i] ? IColumn::PermutationSortDirection::Ascending : IColumn::PermutationSortDirection::Descending,
                IColumn::PermutationSortStability::Stable, 0, 1, p, er
                );
        }

        for (size_t i = 0; i != num_arguments - num_arguments_for_sorting; ++i) {
            MutableColumnPtr permuted_column = IColumn::mutate(data[i]->permute(p, 0));
            ptr[i] = permuted_column.get();
        }

        return ptr;
    }
};


class AggregateFunctionOrderBy final : public IAggregateFunctionHelper<AggregateFunctionOrderBy> {
private:
	AggregateFunctionPtr nested_func;
    DataTypes types_;
    size_t num_arguments;
    String param_str;

public:
    AggregateFunctionOrderBy(AggregateFunctionPtr nested, const DataTypes & types, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionOrderBy>(types, params_)
        , nested_func(nested), num_arguments(types.size())
    {
        if (params_.size() == 0)
            throw Exception("Aggregate function " + getName() + " require at least one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (params_[0].getType() != Field::Types::Which::String)
            throw Exception("First parameter for aggregate function " + getName() + " must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        param_str = params_[0].get<String>();
        types_.reserve(types.size());
        for (size_t i = 0; i != types.size(); ++i) {
            types_.push_back(types[i]->getPtr());
        }
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
        new(place) OrderByData(param_str, types_);    
        nested_func->create(place + sizeof(OrderByData));
    }

    void destroy(AggregateDataPtr place) const noexcept override {
        reinterpret_cast<OrderByData *>(place)->~OrderByData();
        nested_func->destroy(place + sizeof(OrderByData));
    }

    size_t sizeOfData() const override
    {
        return sizeof(OrderByData) + nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return alignof(OrderByData);
    }

    bool hasTrivialDestructor() const override {
        return ((std::is_trivially_destructible_v<OrderByData>) && (nested_func->hasTrivialDestructor()));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override 
    {
        for (size_t i = 0; i != reinterpret_cast<const OrderByData *>(rhs)->data[0]->size(); ++i) { 
            for (size_t j = 0; j != reinterpret_cast<const OrderByData *>(rhs)->data.size(); ++j) {
                reinterpret_cast<OrderByData *>(place)->data[j]->insertFrom(*(reinterpret_cast<const OrderByData *>(rhs)->data[j]), i);
            }         
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override 
    {
        size_t column_size = reinterpret_cast<const OrderByData *>(place)->data[0]->size();
        writeBinary(column_size, buf);
        auto size = reinterpret_cast<const OrderByData *>(place)->data.size();
        for (size_t i = 0; i != size; ++i) {
            for (size_t j = 0; j != column_size; ++j) {
                auto ob_data = reinterpret_cast<const OrderByData *>(place);
                writeBinary((*ob_data->data[i])[j].get<TypeIndexToType<TypeIndex::UInt8>>(), buf);
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        size_t column_size;
        readBinary(column_size, buf);
        auto size = reinterpret_cast<OrderByData *>(place)->data.size();
        for (size_t i = 0; i != size; ++i) {
            for (size_t j = 0; j != column_size; ++j) {
                TypeIndexToType<TypeIndex::UInt8> val; 
                readBinary(val, buf);
                Field f = val;
                reinterpret_cast<OrderByData *>(place)->data[i]->insert(f);
            }
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override 
    {
        for (size_t i = 0; i != num_arguments; ++i) {
            reinterpret_cast<OrderByData *>(place)->data[i]->insertFrom((*columns)[i], row_num);
        }               
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override 
    {
        std::unique_ptr<IColumn*[]> columns = reinterpret_cast<OrderByData *>(place)->sort();

        for (size_t i = 0; i != columns[0]->size(); ++i) {
            IColumn** columns_ptr = columns.get(); 
            nested_func->add(place + sizeof(OrderByData), const_cast<const IColumn**>(columns_ptr), i, arena);
        }

        nested_func->insertResultInto(place + sizeof(OrderByData), to, arena);
    }

    bool allocatesMemoryInArena() const override {
        return nested_func->allocatesMemoryInArena();
    }
};
}
