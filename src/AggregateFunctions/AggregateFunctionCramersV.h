#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <limits>
#include <cmath>

#include <type_traits>


namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{


    struct AggregateFunctionCramersVData
    {
        size_t cur_size = 0;
        HashMap<UInt64, UInt64> n_i;
        HashMap<UInt64, UInt64> n_j;
        HashMap<UInt128, UInt64> pairs;


        void add(UInt64 hash1, UInt64 hash2)
        {
            cur_size += 1;
            n_i[hash1] += 1;
            n_j[hash2] += 1;

            UInt128 hash_pair = hash1 | (static_cast<UInt128>(hash2) << 64);
            pairs[hash_pair] += 1;

        }

        void merge(const AggregateFunctionCramersVData &other)
        {
            cur_size += other.cur_size;

            for (const auto& pair : other.n_i) {
                UInt64 hash1 = pair.getKey();
                UInt64 count = pair.getMapped();
                n_i[hash1] += count;
            }
            for (const auto& pair : other.n_j) {
                UInt64 hash1 = pair.getKey();
                UInt64 count = pair.getMapped();
                n_j[hash1] += count;
            }
            for (const auto& pair : other.pairs) {
                UInt128 hash1 = pair.getKey();
                UInt64 count = pair.getMapped();
                pairs[hash1] += count;
            }
        }

        void serialize(WriteBuffer &buf) const
        {
            writeBinary(cur_size, buf);
            n_i.write(buf);
            n_j.write(buf);
            pairs.write(buf);
        }

        void deserialize(ReadBuffer &buf)
        {
            readBinary(cur_size, buf);
            n_i.read(buf);
            n_j.read(buf);
            pairs.read(buf);
        }

        Float64 get_result() const
        {
            if (cur_size < 2){
                throw Exception("Aggregate function cramer's v requires et least 2 values in columns", ErrorCodes::BAD_ARGUMENTS);
            }
            Float64 phi = 0.0;
            for (const auto & cell : pairs) {
                UInt128 hash_pair = cell.getKey();
                UInt64 count_of_pair_tmp = cell.getMapped();
                Float64 count_of_pair = Float64(count_of_pair_tmp);
                UInt64 hash1 = (hash_pair << 64 >> 64);
                UInt64 hash2 = (hash_pair >> 64);

                UInt64 count1_tmp = n_i.find(hash1)->getMapped();
                UInt64 count2_tmp = n_j.find(hash2)->getMapped();
                Float64 count1 = static_cast<Float64>(count1_tmp);
                Float64 count2 = Float64(count2_tmp);

                phi += ((count_of_pair * count_of_pair / (count1 * count2) * cur_size)
                        - 2 * count_of_pair + (count1 * count2 / cur_size));
            }
            phi /= cur_size;

            UInt64 q = std::min(n_i.size(), n_j.size());
            phi /= (q - 1);
            return sqrt(phi);

        }
    };

    template <typename Data>

    class AggregateFunctionCramersV : public
                                        IAggregateFunctionDataHelper<
                                                Data,
                                                AggregateFunctionCramersV<Data>
                                        >
    {

    public:
        AggregateFunctionCramersV(
                const DataTypes & arguments
        ):
                IAggregateFunctionDataHelper<
                        Data,
                        AggregateFunctionCramersV<Data>
                > ({arguments}, {})
        {
            // notice: arguments has been in factory
        }

        String getName() const override
        {
            return "CramersV";
        }

        bool allocatesMemoryInArena() const override { return false; }

        DataTypePtr getReturnType() const override
        {
            return std::make_shared<DataTypeNumber<Float64>>();
        }

        void add(
                AggregateDataPtr __restrict place,
                const IColumn ** columns,
                size_t row_num,
                Arena *
        ) const override
        {
            UInt64 hash1 = UniqVariadicHash<false, false>::apply(1, columns, row_num);
            UInt64 hash2 = UniqVariadicHash<false, false>::apply(1, columns + 1, row_num);

            this->data(place).add(hash1, hash2);
        }

        void merge(
                AggregateDataPtr __restrict place,
                ConstAggregateDataPtr rhs, Arena *
        ) const override
        {
            this->data(place).merge(this->data(rhs));
        }

        void serialize(
                ConstAggregateDataPtr __restrict place,
                WriteBuffer & buf
        ) const override
        {
            this->data(place).serialize(buf);
        }

        void deserialize(
                AggregateDataPtr __restrict place,
                ReadBuffer & buf, Arena *
        ) const override
        {
            this->data(place).deserialize(buf);
        }

        void insertResultInto(
                AggregateDataPtr __restrict place,
                IColumn & to,
                Arena *
        ) const override
        {
            Float64 result = this->data(place).get_result();
//            std::cerr << "cur_size" << this->data(place).cur_size  << '\n';
//            std::cerr << "n_i size" << this->data(place).n_i.size()  << '\n';
//            std::cerr << "n_j size" << this->data(place).n_j.size() << '\n';
//            std::cerr << "pair size " << this->data(place).pairs.size()  << '\n';
//            std::cerr << "result " << result << '\n';

            auto & column = static_cast<ColumnVector<Float64> &>(to);
            column.getData().push_back(result);
        }

    };

}
