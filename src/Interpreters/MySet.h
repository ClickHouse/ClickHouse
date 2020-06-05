#pragma once

#include <shared_mutex>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Common/ProbabilisticDataFilters/BasicBloomFilter.h>
#include <Common/ProbabilisticDataFilters/ChaoticFilter.h>
#include <Core/Block.h>
#include <DataStreams/SizeLimits.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/BloomFilterHash.h>
#include <Interpreters/Context.h>
#include <Interpreters/ISet.h>
#include <Interpreters/SetVariants.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/BoolMask.h>

#include <common/logger_useful.h>

#include <unordered_set>


namespace DB
{

struct Range;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class MySet : public ISet
{
public:
    MySet(const SizeLimits&, bool, bool, size_t filter_length_, size_t hashes_count_)
        : log(&Logger::get("MySet")), filter_length(filter_length_), hashes_count(hashes_count_)
    {
    }

    bool empty() const override {
        std::unique_lock lock(rwlock);
        return !hash_set || hash_set->added_counts() == 0;
    }

    void setHeader(const Block & header) override {
        std::unique_lock lock(rwlock);

        // std::cerr << "HERE in setHeader, rows: " << header.rows() << std::endl;

        columns_count = header.columns();

        if (columns_count == 0) return;

        hash_set = std::make_shared<BasicBloomFilter>(filter_length, hashes_count);
        // hash_set = std::make_shared<ChaoticFilter>(filter_length);

        std::cerr << "filter_length: " << filter_length << " ," << "hashes_count: " << hashes_count << std::endl;
        for (size_t i = 0; i < columns_count; ++i) {
            data_types.emplace_back(header.safeGetByPosition(i).type);
        }
    }

    bool insertFromBlock(const Block & block) override
    {
        std::unique_lock lock(rwlock);
        // // // std::cerr << "HERE in insertFromBlock\n";

        const auto row_hashes = calculate_hashes(block);

        // const auto& col = typeid_cast<const ColumnVector<char8_t>&>(*block.getByPosition(0).column);

        for (const auto & elem : row_hashes) {
            hash_set->add(elem);
        }

        return true;
    }

    ColumnPtr execute(const Block & block, bool negative) const override {
        std::shared_lock lock(rwlock);
        // // // std::cerr << "HERE in execute\n";

        const auto row_hashes = calculate_hashes(block);

        auto res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = res->getData();
        vec_res.resize(block.safeGetByPosition(0).column->size());

        size_t i = 0;
        for (const auto & elem : row_hashes) {
            if (hash_set->contains(elem)) {
                vec_res[i] = 1 ^ negative;
            } else {
                vec_res[i] = negative;
            }
            ++i;
        }
        return res;
    }

    void finishInsert() override {
        std::shared_lock lock(rwlock);
        // // // std::cerr << "HERE in finishInsert, size of hashset: " + std::to_string(hash_set->added_counts()) + "\n";
        is_created = true;
    }
    bool isCreated() const override { return is_created; }
    size_t getTotalRowCount() const override
    {
        std::unique_lock lock(rwlock);
        return hash_set->added_counts();
    }

    const DataTypes & getDataTypes() const override { return data_types; }

private:
    ColumnUInt64::Container calculate_hashes(const Block & block) const {
        ColumnUInt64::Container row_hashes(block.rows(), 0);

        // // // std::cerr << "HERE in calculate_hashes, columns_count : " + std::to_string(columns_count) + "\n";

        for (size_t column_index = 0; column_index < columns_count; ++column_index)
        {
            if (column_index == 0)
            {
                BloomFilterHash::getAnyTypeHash<true>(
                    block.getByPosition(column_index).type.get(),
                    block.getByPosition(column_index).column.get(),
                    row_hashes, 0
                );
            }
            else
            {
                BloomFilterHash::getAnyTypeHash<false>(
                    block.getByPosition(column_index).type.get(),
                    block.getByPosition(column_index).column.get(),
                    row_hashes, 0
                );
            }
        }

        return row_hashes;
    }


private:
    Logger * log;

    mutable std::shared_mutex rwlock;
    BasicBloomFilterPtr hash_set;
    // ChaoticFilterPtr hash_set;

    size_t columns_count = 0;
    bool is_created = false;
    size_t filter_length;
    size_t hashes_count;

    DataTypes data_types;
};

}  // namespace DB
