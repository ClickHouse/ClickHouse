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

/// Implementation of entropy-learned hashing: https://dl.acm.org/doi/10.1145/3514221.3517894
/// TODOs for future work:
/// - allow to specify an arbitrary hash function (currently always CityHash is used)
/// - allow function chaining a la entropyLearnedHash(trainEntropyLearnedHash())


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

using Key = String;
using PartialKeyPositions = std::vector<size_t>;

namespace
{

Key getPartialKey(const Key & key, const PartialKeyPositions & partial_key_positions)
{
    Key result_key;
    result_key.reserve(partial_key_positions.size());
    for (auto partial_key_position : partial_key_positions)
        if (partial_key_position < key.size())
            result_key.push_back(key[partial_key_position]);
    return result_key;
}

bool allPartialKeysAreUnique(const std::vector<Key> & data, const PartialKeyPositions & partial_key_positions)
{
    std::unordered_set<Key> partial_keys;
    partial_keys.reserve(data.size());
    for (const auto & key : data)
        if (!partial_keys.insert(getPartialKey(key, partial_key_positions)).second)
            return false;
    return true;
}

// NextByte returns position of byte which adds the most entropy and the new entropy
std::pair<size_t, size_t> nextByte(const std::vector<Key> & keys, size_t max_len, std::vector<size_t> & chosen_bytes)
{
    size_t min_collisions = std::numeric_limits<size_t>::max();
    size_t best_position = 0;

    std::unordered_map<Key, size_t> count_table;
    for (size_t i = 0; i < max_len; ++i)
    {
        count_table.clear();
        count_table.reserve(keys.size());

        chosen_bytes.push_back(i);
        size_t collisions = 0;
        for (const auto & key : keys)
        {
            auto partial_key = getPartialKey(key, chosen_bytes);
            collisions += count_table[partial_key]++;
        }

        if (collisions < min_collisions)
        {
            min_collisions = collisions;
            best_position = i;
        }
        chosen_bytes.pop_back();
    }
    return {best_position, min_collisions};
}

// std::pair<size_t, size_t> nextByte(const std::vector<Key> & keys, std::vector<size_t> & chosen_bytes)
// {
//     size_t max_len = 0;
//     for (const auto & key : keys)
//         max_len = std::max(max_len, key.size());

//     return nextByte(keys, max_len, chosen_bytes);
// }

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
        /// partial_key_positions_by_id[id] = partial_key_positions;
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

std::pair<PartialKeyPositions, std::vector<size_t>> chooseBytes(const std::vector<Key> & train_data)
{
    if (train_data.size() <= 1)
        return {};

    // position contains numbers of chosen bytes
    PartialKeyPositions partial_key_positions;

    // entropies contains entropies of keys after each new chosen byte
    std::vector<size_t> entropies;

    // max_len is a maximal length of any key in train_data
    size_t max_len = 0;
    for (const auto & key : train_data)
        max_len = std::max(max_len, key.size());

    // while not all partial keys unique, choose new byte and recalculate the entropy
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

            std::vector<Key> training_data;
            for (size_t i = 0; i < num_rows; ++i)
            {
                std::string_view string_ref = col_data_string->getDataAt(i).toView();
                training_data.emplace_back(string_ref.data(), string_ref.size());
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
            for (size_t i = 0; i < num_rows; ++i)
            {
                std::string_view string_ref = col_data_string->getDataAt(i).toView();
                Key key(string_ref.data(), string_ref.size());
                Key partial_key = getPartialKey(key, partial_key_positions);
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
