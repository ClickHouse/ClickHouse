#include <base/defines.h>
#include <base/types.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

/// Implementation of entropy-learned hashing: https://doi.org/10.1145/3514221.3517894
/// If you change something in this file, please don't deviate too much from the pseudocode in the paper!

/// TODOs for future work:
/// - allow to specify an arbitrary hash function (currently always CityHash is used)
/// - allow function chaining a la entropyLearnedHash(trainEntropyLearnedHash())
/// - support more datatypes for data (besides String)


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

using PartialKeyPositions = std::vector<size_t>;
using Entropies = std::vector<size_t>;

void getPartialKey(std::string_view key, const PartialKeyPositions & partial_key_positions, String & result)
{
    result.clear();
    result.reserve(partial_key_positions.size());

    for (auto partial_key_position : partial_key_positions)
        if (partial_key_position < key.size())
            result.push_back(key[partial_key_position]);
}

bool allPartialKeysAreUnique(const std::vector<std::string_view> & keys, const PartialKeyPositions & partial_key_positions)
{
    std::unordered_set<String> unique_partial_keys;
    unique_partial_keys.reserve(keys.size());
    String partial_key;

    for (const auto & key : keys)
    {
        getPartialKey(key, partial_key_positions, partial_key);
        if (!unique_partial_keys.insert(partial_key).second)
            return false;
    }

    return true;
}

// NextByte returns position of byte which adds the most entropy and the new entropy
std::pair<size_t, size_t> nextByte(const std::vector<std::string_view> & keys, size_t max_len, PartialKeyPositions & partial_key_positions)
{
    size_t min_collisions = std::numeric_limits<size_t>::max();
    size_t best_position = 0;

    std::unordered_map<String, size_t> count_table;
    count_table.reserve(keys.size());

    String partial_key;

    for (size_t i = 0; i < max_len; ++i)
    {
        count_table.clear();

        partial_key_positions.push_back(i);
        size_t collisions = 0;
        for (const auto & key : keys)
        {
            getPartialKey(key, partial_key_positions, partial_key);
            collisions += count_table[partial_key]++;
        }

        if (collisions < min_collisions)
        {
            min_collisions = collisions;
            best_position = i;
        }
        partial_key_positions.pop_back();
    }

    return {best_position, min_collisions};
}

std::pair<PartialKeyPositions, Entropies> chooseBytes(const std::vector<std::string_view> & train_data)
{
    if (train_data.size() <= 1)
        return {};

    PartialKeyPositions partial_key_positions;
    Entropies entropies;

    size_t max_len = 0; /// length of the longest key in training data
    for (const auto & key : train_data)
        max_len = std::max(max_len, key.size());

    while (!allPartialKeysAreUnique(train_data, partial_key_positions))
    {
        auto [new_position, new_entropy] = nextByte(train_data, max_len, partial_key_positions);
        if (!entropies.empty() && new_entropy == entropies.back())
            break;
        partial_key_positions.push_back(new_position);
        entropies.push_back(new_entropy);
    }
    return {partial_key_positions, entropies};
}

class IdManager
{
public:
    static IdManager & instance()
    {
        static IdManager instance;
        return instance;
    }
    void setPartialKeyPositionsForId(const String & user_name, const String & id, const PartialKeyPositions & partial_key_positions)
    {
        std::lock_guard lock(mutex);
        auto & ids_for_user = partial_key_positions_by_id[user_name];
        ids_for_user[id] = partial_key_positions;
    }
    const PartialKeyPositions & getPartialKeyPositionsForId(const String & user_name, const String & id) const
    {
        std::lock_guard lock(mutex);
        auto it_user = partial_key_positions_by_id.find(user_name);
        if (it_user == partial_key_positions_by_id.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Id {} not registered for user in entropy learned hashing", id);
        auto it_id = it_user->second.find(id);
        if (it_id == it_user->second.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Id {} not registered for user in entropy learned hashing", id);
        return it_id->second;
    }

private:
    mutable std::mutex mutex;
    /// Map: user name --> (Map: dataset id --> byte positions to hash)
    std::map<String, std::map<String, PartialKeyPositions>> partial_key_positions_by_id TSA_GUARDED_BY(mutex);
};

}

class FunctionTrainEntropyLearnedHash : public IFunction
{
public:
    static constexpr auto name = "trainEntropyLearnedHash";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTrainEntropyLearnedHash>(context->getUserName()); }
    explicit FunctionTrainEntropyLearnedHash(const String & user_name_) : IFunction(), user_name(user_name_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"data", &isString<IDataType>, nullptr, "String"},
            {"id", &isString<IDataType>, nullptr, "String"}
        };

        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        const IColumn * id_col = arguments[1].column.get();
        const ColumnConst * id_col_const = checkAndGetColumn<ColumnConst>(id_col);
        const String id = id_col_const->getValue<String>();

        const auto * data_col = arguments[0].column.get();
        if (const ColumnString * col_data_string = checkAndGetColumn<ColumnString>(data_col))
        {
            const size_t num_rows = col_data_string->size();

            /// TODO this does some needless copying ... chooseBytes() should ideally understand the native ColumnString representation
            std::vector<std::string_view> training_data;
            for (size_t i = 0; i < num_rows; ++i)
            {
                std::string_view string_view = col_data_string->getDataAt(i).toView();
                training_data.emplace_back(string_view);
            }

            PartialKeyPositions partial_key_positions = chooseBytes(training_data).first;
            auto & id_manager = IdManager::instance();
            id_manager.setPartialKeyPositionsForId(user_name, id, partial_key_positions);

            return result_type->createColumnConst(num_rows, 0u)->convertToFullColumnIfConst();
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments.begin()->column->getName(), getName());
    }
private:
    const String user_name;
};


class FunctionEntropyLearnedHash : public IFunction
{
public:
    static constexpr auto name = "entropyLearnedHash";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionEntropyLearnedHash>(context->getUserName()); }
    explicit FunctionEntropyLearnedHash(const String & user_name_) : IFunction(), user_name(user_name_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }


    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"data", &isString<IDataType>, nullptr, "String"},
            {"id", &isString<IDataType>, nullptr, "String"}
        };

        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const IColumn * id_col = arguments.back().column.get();
        const ColumnConst * id_col_const = checkAndGetColumn<ColumnConst>(id_col);
        const String id = id_col_const->getValue<String>();

        const auto & id_manager = IdManager::instance();
        const auto & partial_key_positions = id_manager.getPartialKeyPositionsForId(user_name, id);

        const auto * data_col = arguments[0].column.get();
        if (const auto * col_data_string = checkAndGetColumn<ColumnString>(data_col))
        {
            const size_t num_rows = col_data_string->size();
            auto col_res = ColumnUInt64::create(num_rows);

            auto & col_res_vec = col_res->getData();
            String partial_key;
            for (size_t i = 0; i < num_rows; ++i)
            {
                std::string_view string_ref = col_data_string->getDataAt(i).toView();
                getPartialKey(string_ref, partial_key_positions, partial_key);
                col_res_vec[i] = CityHash_v1_0_2::CityHash64(partial_key.data(), partial_key.size());
            }

            return col_res;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments.begin()->column->getName(), getName());
    }
private:
    const String user_name;
};

REGISTER_FUNCTION(EntropyLearnedHash)
{
    factory.registerFunction<FunctionTrainEntropyLearnedHash>();
    factory.registerFunction<FunctionEntropyLearnedHash>();
}

}
