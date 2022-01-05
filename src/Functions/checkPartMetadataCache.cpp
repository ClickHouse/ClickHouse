#include "config_core.h"

#if USE_ROCKSDB
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Common/hex.h>
#include <Core/Field.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionCheckPartMetadataCache : public IFunction, WithContext
{
public:
    using uint128 = IMergeTreeDataPart::uint128;
    using DataPartPtr = MergeTreeData::DataPartPtr;
    using DataPartState = MergeTreeData::DataPartState;
    using DataPartStates = MergeTreeData::DataPartStates;


    static constexpr auto name = "checkPartMetadataCache";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionCheckPartMetadataCache>(context_); }

    static constexpr DataPartStates part_states
        = {DataPartState::Active,
           DataPartState::Temporary,
           DataPartState::PreActive,
           DataPartState::Outdated,
           DataPartState::Deleting,
           DataPartState::DeleteOnDestroy};

    explicit FunctionCheckPartMetadataCache(ContextPtr context_) : WithContext(context_) { }

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isDeterministicInScopeOfQuery() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & argument : arguments)
        {
            if (!isString(argument))
                throw Exception("The argument of function " + getName() + " must have String type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        DataTypePtr key_type = std::make_unique<DataTypeString>();
        DataTypePtr state_type = std::make_unique<DataTypeString>();
        DataTypePtr cache_checksum_type = std::make_unique<DataTypeFixedString>(32);
        DataTypePtr disk_checksum_type = std::make_unique<DataTypeFixedString>(32);
        DataTypePtr match_type = std::make_unique<DataTypeUInt8>();
        DataTypePtr tuple_type
            = std::make_unique<DataTypeTuple>(DataTypes{key_type, state_type, cache_checksum_type, disk_checksum_type, match_type});
        return std::make_shared<DataTypeArray>(tuple_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Get database name
        const auto * arg_database = arguments[0].column.get();
        const ColumnString * column_database = checkAndGetColumnConstData<ColumnString>(arg_database);
        if (!column_database)
            throw Exception("The first argument of function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);
        String database_name = column_database->getDataAt(0).toString();

        /// Get table name
        const auto * arg_table = arguments[1].column.get();
        const ColumnString * column_table = checkAndGetColumnConstData<ColumnString>(arg_table);
        if (!column_table)
            throw Exception("The second argument of function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);
        String table_name = column_table->getDataAt(0).toString();

        /// Get storage
        StorageID storage_id(database_name, table_name);
        auto storage = DatabaseCatalog::instance().getTable(storage_id, getContext());
        auto data = std::dynamic_pointer_cast<MergeTreeData>(storage);
        if (!data || !data->getSettings()->use_metadata_cache)
            throw Exception("The table in function " + getName() + " must be in MergeTree Family", ErrorCodes::ILLEGAL_COLUMN);

        /// Fill in checking results.
        auto col_result = result_type->createColumn();
        auto & col_arr = assert_cast<ColumnArray &>(*col_result);
        auto & col_tuple = assert_cast<ColumnTuple &>(col_arr.getData());
        col_tuple.reserve(data->fileNumberOfDataParts(part_states));
        auto & col_key = assert_cast<ColumnString &>(col_tuple.getColumn(0));
        auto & col_state = assert_cast<ColumnString &>(col_tuple.getColumn(1));
        auto & col_cache_checksum = assert_cast<ColumnFixedString &>(col_tuple.getColumn(2));
        auto & col_disk_checksum = assert_cast<ColumnFixedString &>(col_tuple.getColumn(3));
        auto & col_match = assert_cast<ColumnUInt8 &>(col_tuple.getColumn(4));
        auto parts = data->getDataParts(part_states);
        for (const auto & part : parts)
            executePart(part, col_key, col_state, col_cache_checksum, col_disk_checksum, col_match);
        col_arr.getOffsets().push_back(col_tuple.size());
        return result_type->createColumnConst(input_rows_count, col_arr[0]);
    }

    static void executePart(
        const DataPartPtr & part,
        ColumnString & col_key,
        ColumnString & col_state,
        ColumnFixedString & col_cache_checksum,
        ColumnFixedString & col_disk_checksum,
        ColumnUInt8 & col_match)
    {
        Strings keys;
        auto state_view = part->stateString();
        String state(state_view.data(), state_view.size());
        std::vector<uint128> cache_checksums;
        std::vector<uint128> disk_checksums;
        uint8_t match = 0;
        size_t file_number = part->fileNumberOfColumnsChecksumsIndexes();
        keys.reserve(file_number);
        cache_checksums.reserve(file_number);
        disk_checksums.reserve(file_number);

        // part->checkMetadataCache(keys, cache_checksums, disk_checksums);
        for (size_t i = 0; i < keys.size(); ++i)
        {
            col_key.insert(keys[i]);
            col_state.insert(state);
            col_cache_checksum.insert(getHexUIntUppercase(cache_checksums[i].first) + getHexUIntUppercase(cache_checksums[i].second));
            col_disk_checksum.insert(getHexUIntUppercase(disk_checksums[i].first) + getHexUIntUppercase(disk_checksums[i].second));

            match = cache_checksums[i] == disk_checksums[i] ? 1 : 0;
            col_match.insertValue(match);
        }
    }
};

void registerFunctionCheckPartMetadataCache(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCheckPartMetadataCache>();
}
}
#endif
