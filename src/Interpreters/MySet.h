#pragma once

#include <shared_mutex>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Common/ProbabilisticDataFilters/BasicBloomFilter.h>
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
    MySet(const SizeLimits&, bool, bool)
        : log(&Logger::get("MySet")),
          hash_set(4096 * 10, 12)
    {
    }

    bool empty() const override {
        std::cerr << "HERE in empty\n";
        // return hash_set.empty();
        return hash_set.added_counts() == 0;
    }

    void setHeader(const Block & header) override {
        std::cerr << "HERE in setHeader\n";

        columns_count = header.columns();
        // if (keys_size != 1) {
        //     throw Exception("keys_size != 1", ErrorCodes::LOGICAL_ERROR);
        // }
        // if (!WhichDataType(header.getByPosition(0).type).isUInt8()) {
        // if (!WhichDataType(header.getByPosition(0).type).isUInt64()) {
        //     throw Exception("Column should be ui8", ErrorCodes::LOGICAL_ERROR);
        // }
        for (size_t i = 0; i < columns_count; ++i) {
            data_types.emplace_back(header.safeGetByPosition(i).type);
        }
    }

    bool insertFromBlock(const Block & block) override
    {
        std::cerr << "HERE in insertFromBlock\n";

        const auto row_hashes = calculate_hashes(block);

        // const auto& col = typeid_cast<const ColumnVector<char8_t>&>(*block.getByPosition(0).column);

        for (const auto & elem : row_hashes) {
            // hash_set.insert(elem);
            hash_set.add(elem);
        }

        return true;
    }

    ColumnPtr execute(const Block & block, bool negative) const override {
        std::cerr << "HERE in execute\n";

        const auto row_hashes = calculate_hashes(block);

        auto res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = res->getData();
        vec_res.resize(block.safeGetByPosition(0).column->size());

        size_t i = 0;
        for (const auto & elem : row_hashes) {
            // if (hash_set.count(elem)) {
            if (hash_set.contains(elem)) {
                vec_res[i] = 1 ^ negative;
            } else {
                vec_res[i] = negative;
            }
            ++i;
        }
        return res;
    }

    void finishInsert() override {
        // std::cerr << "HERE in finishInsert, size of hashset: " + std::to_string(hash_set.size()) + "\n";
        std::cerr << "HERE in finishInsert, size of hashset: " + std::to_string(hash_set.added_counts()) + "\n";
        is_created = true;
    }
    bool isCreated() const override { return is_created; }
    size_t getTotalRowCount() const override
    {
        // return hash_set.size();
        return hash_set.added_counts();
    }

    const DataTypes & getDataTypes() const override { return data_types; }

private:
    ColumnUInt64::Container calculate_hashes(const Block & block) const {
        ColumnUInt64::Container row_hashes(block.rows(), 0);

        std::cerr << "HERE in calculate_hashes, columns_count : " + std::to_string(columns_count) + "\n";

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

    // std::unordered_set <uint64_t> hash_set;
    BasicBloomFilter hash_set;

    size_t columns_count;
    bool is_created = false;

    DataTypes data_types;
};

}  // namespace DB
