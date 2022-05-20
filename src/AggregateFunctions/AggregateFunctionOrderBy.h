#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/TypeId.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>

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

    std::vector<String> join(const String& s) {
        String curr = "";
        std::vector<String> ans;
        size_t i = 0;
        while (i < s.size()) {
            if (s[i] == ',') {
                ans.push_back(curr);
                curr = "";
                if ((i + 1 < s.size()) && (s[i + 1] == ' ')) {
                    i += 2;
                } else {
                    i++;
                }
            } else {
                curr.push_back(s[i]);
                i++;
            }
        }
        ans.push_back(curr);
        return ans;
    }

    void parseString(const String& s)
    {
        auto strs = join(s);
        for (String& str : strs) {
            std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) -> unsigned char { return std::toupper(c); });
            auto pos = str.find("DESC");
            if ((pos != std::string::npos) && (pos != 0)) {
                bitmap.push_back(false);
            } else {
                bitmap.push_back(true);
            }
        }
        num_arguments_for_sorting = bitmap.size();
    }

    OrderByData(const String& param_str, const DataTypes& types)   
    {
        num_arguments = types.size();
        parseString(param_str);
        data.reserve(num_arguments);
        for (size_t i = 0; i != num_arguments; ++i) {
            data.push_back(types[i]->createColumn());
        }
    }

    std::vector<MutableColumnPtr> sort() {
        IColumn::Permutation p;
        for (size_t i = 0; i != data[0]->size(); ++i) {
            p.push_back(i);
        }

        std::vector<MutableColumnPtr> vec; 
        EqualRanges er(1, std::pair<size_t, size_t>(0, data[0]->size()));

        for (size_t i = num_arguments - num_arguments_for_sorting; i != num_arguments; ++i) {
            std::cerr << bitmap[i - num_arguments + num_arguments_for_sorting] << std::endl;
            data[i]->updatePermutation(
                bitmap[i - num_arguments + num_arguments_for_sorting] ? IColumn::PermutationSortDirection::Ascending : IColumn::PermutationSortDirection::Descending,
                IColumn::PermutationSortStability::Stable, 0, 1, p, er
                );
        }

        for (size_t i = 0; i != num_arguments - num_arguments_for_sorting; ++i) {
            vec.push_back(IColumn::mutate(data[i]->permute(p, 0)));
        }

        return vec;
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
        for (size_t i = 0; i != reinterpret_cast<const OrderByData *>(rhs)->data.size(); ++i) {
            size_t sz = reinterpret_cast<const OrderByData *>(rhs)->data[0]->size();
            reinterpret_cast<OrderByData *>(place)->data[i]->insertRangeFrom(*(reinterpret_cast<const OrderByData *>(rhs)->data[i]), 0, sz);
        }         
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override 
    {
        for (size_t i = 0; i != reinterpret_cast<const OrderByData *>(place)->data.size(); ++i) {
            const MutableColumnPtr& mutable_column = reinterpret_cast<const OrderByData *>(place)->data[i];
            size_t sz = reinterpret_cast<const OrderByData *>(place)->data[i]->size();
            ColumnPtr column(mutable_column->cloneResized(sz));
            auto serialization = types_[i]->getDefaultSerialization();

            ISerialization::SerializeBinaryBulkSettings settings;
            settings.getter = [&buf](ISerialization::SubstreamPath) -> WriteBuffer * { return &buf; };
            settings.position_independent_encoding = false;
            settings.low_cardinality_max_dictionary_size = 0;

            ISerialization::SerializeBinaryBulkStatePtr state;
            serialization->serializeBinaryBulkStatePrefix(settings, state);
            serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, sz, settings, state);
            serialization->serializeBinaryBulkStateSuffix(settings, state);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        for (size_t i = 0; i != reinterpret_cast<const OrderByData *>(place)->data.size(); ++i) {
            MutableColumnPtr& mutable_column = reinterpret_cast<OrderByData *>(place)->data[i];
            size_t sz = reinterpret_cast<OrderByData *>(place)->data[i]->size();
            ColumnPtr column(mutable_column->cloneResized(sz));
            auto serialization = types_[i]->getDefaultSerialization();
            
            ISerialization::DeserializeBinaryBulkSettings settings;
            settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &buf; };
            settings.avg_value_size_hint = 0;
            settings.position_independent_encoding = false;
            settings.native_format = true;

            ISerialization::DeserializeBinaryBulkStatePtr state;

            serialization->deserializeBinaryBulkStatePrefix(settings, state);
            serialization->deserializeBinaryBulkWithMultipleStreams(column, sz, settings, state, nullptr);
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override 
    {
        for (size_t i = 0; i != num_arguments; ++i) {
            reinterpret_cast<OrderByData *>(place)->data[i]->insertFrom(*columns[i], row_num);
        }               
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override 
    {
        std::vector<MutableColumnPtr> columns = reinterpret_cast<OrderByData *>(place)->sort();

        IColumn* columns_ptr[columns.size()];
        for (size_t i = 0; i != columns.size(); ++i) {
            columns_ptr[i] = columns[i].get();
        }

        for (size_t i = 0; i != columns[0]->size(); ++i) { 
            nested_func->add(place + sizeof(OrderByData), const_cast<const IColumn**>(columns_ptr), i, arena);
        }

        nested_func->insertResultInto(place + sizeof(OrderByData), to, arena);
    }

    bool allocatesMemoryInArena() const override {
        return nested_func->allocatesMemoryInArena();
    }
};
}
