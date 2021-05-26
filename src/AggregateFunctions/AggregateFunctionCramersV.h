#pragma once

#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <type_traits>
#include <city.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <Common/assert_cast.h>
#include <Common/PODArray_fwd.h>
#include <Common/HashTable/HashMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <common/types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <ext/bit_cast.h>
#include <Interpreters/AggregationCommon.h>
#include "Core/DecimalFunctions.h"


namespace DB
{
    using Map1 = HashMap<UInt64, UInt64>;
    using Map2 = HashMap<UInt64, UInt64>;
    using Map12 = HashMap<UInt128, UInt64>;


    struct AggregateFunctionCramersVData
    {
        UInt64 size_of_column = 0;
        Map1 count_of_value1;
        Map2 count_of_value2;
        Map12 count_of_pairs;
        Float64 NO_SANITIZE_UNDEFINED getResult() const
        {
            UInt64 q = std::min(count_of_value1.size(), count_of_value2.size());
            if (q == 1) {
                return 1;
            }
            Float64 chi_squared = 0;
            for (const auto & cell : count_of_pairs) {
                UInt128 hash_pair = cell.getKey();
                UInt64 hash1 = (hash_pair << 64 >> 64);
                UInt64 hash2 = (hash_pair >> 64);
                UInt64 count1 = count_of_value1.find(hash1)->getMapped();
                UInt64 count2 = count_of_value2.find(hash2)->getMapped();
                UInt64 count_of_pair = cell.getMapped();
                Float64 expectation = static_cast<Float64>(count1) * count2 / size_of_column;
                chi_squared += (count_of_pair - expectation) * (count_of_pair - expectation) / expectation;

            }
            return sqrt(chi_squared / (q - 1) / size_of_column);
        }

        static constexpr UInt32 num_args = 2;
    };


    class AggregateFunctionCramersV :
            public IAggregateFunctionDataHelper<AggregateFunctionCramersVData, AggregateFunctionCramersV>
    {
    public:
        explicit AggregateFunctionCramersV(const DataTypes & arguments)
                :IAggregateFunctionDataHelper<AggregateFunctionCramersVData, AggregateFunctionCramersV> ({arguments}, {})
        {}

        String getName() const override
        {
            return "CramersV";
        }

        bool allocatesMemoryInArena() const override { return false; } // ????

        DataTypePtr getReturnType() const override
        {
            return std::make_shared<DataTypeNumber<Float64>>();
        }

        void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
        {
            ++this->data(place).size_of_column;
            UInt64 hash1 = UniqVariadicHash<false, false>::apply(1, columns, row_num);
            UInt64 hash2 = UniqVariadicHash<false, false>::apply(1, columns + 1, row_num);
            UInt128 hash_pair = hash1 | (static_cast<UInt128>(hash2) << 64);
            this->data(place).count_of_value1[hash1] += 1;
            this->data(place).count_of_value2[hash2] += 1;
            this->data(place).count_of_pairs[hash_pair] += 1;
        }
        void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
        {
            this->data(place).size_of_column += this->data(rhs).size_of_column;
            for (const auto& pair : this->data(rhs).count_of_value1) {
                UInt64 hash1 = pair.getKey();
                UInt64 count = pair.getMapped();
                this->data(place).count_of_value1[hash1] += count;
            }
            for (const auto& pair : this->data(rhs).count_of_value2) {
                UInt64 hash2 = pair.getKey();
                UInt64 count = pair.getMapped();
                this->data(place).count_of_value2[hash2] += count;
            }
            for (const auto & pair : this->data(rhs).count_of_pairs) {
                UInt128 hash_pair = pair.getKey();
                UInt64 count_of_pair = pair.getMapped();
                this->data(place).count_of_pairs[hash_pair] += count_of_pair;
            }
        }

        void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
        {
            writeVarUInt(data(place).size_of_column, buf);
            this->data(place).count_of_value1.write(buf);
            this->data(place).count_of_value2.write(buf);
            this->data(place).count_of_pairs.write(buf);
        }

        void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
        {
            readVarUInt(this->data(place).size_of_column, buf);
            this->data(place).count_of_value1.read(buf);
            this->data(place).count_of_value2.read(buf);
            this->data(place).count_of_pairs.read(buf);
        }

        void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
        {
            auto answer = this->data(place).getResult();
            auto & column = static_cast<ColumnVector<Float64> &>(to);
            column.getData().push_back(answer);
        }
    };

}
