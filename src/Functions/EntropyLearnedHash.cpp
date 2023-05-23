#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsHashing.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

/// Implementation of entropy-learned hashing: https://doi.org/10.1145/3514221.3517894
/// If you change something in this file, please don't deviate too much from the pseudocode in the paper!

/// TODOs for future work:
/// - allow to specify an arbitrary hash function (currently always CityHash is used)
/// - allow function chaining a la entropyLearnedHash('column', trainEntropyLearnedHash('column'))
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
    std::pair<size_t, size_t>
    nextByte(const std::vector<std::string_view> & keys, size_t max_len, PartialKeyPositions & partial_key_positions)
    {
        size_t min_collisions = std::numeric_limits<size_t>::max();
        size_t best_position = 0;

        std::unordered_map<String, size_t> count_table;
        count_table.reserve(keys.size());

        String partial_key;

        // Optimization: assume here that partial_key_positions is sorted
        // and use two pointers technique to avoid checking already taken positions.
        for (size_t i = 0, partial_key_positions_pos = 0; i < max_len; ++i)
        {
            if (partial_key_positions_pos < partial_key_positions.size() && partial_key_positions[partial_key_positions_pos] == i)
            {
                ++partial_key_positions_pos;
                continue;
            }

            count_table.clear();

            // It's not important here to insert i to partial_key_positions to keep it sorted,
            // because in the end of the loop body it will still be popped.
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

        // Just return the best found position, partial_key_positions is not changed. best_position will be inserted to it later.
        return {best_position, min_collisions};
    }

    PartialKeyPositions chooseBytes(const std::vector<std::string_view> & train_data)
    {
        if (train_data.size() <= 1)
            return {};

        PartialKeyPositions partial_key_positions;
        size_t last_entropy = 0;

        size_t max_len = 0; /// length of the longest key in training data
        for (const auto & key : train_data)
            max_len = std::max(max_len, key.size());

        while (!allPartialKeysAreUnique(train_data, partial_key_positions))
        {
            auto [new_position, new_entropy] = nextByte(train_data, max_len, partial_key_positions);
            if (last_entropy > 0 && new_entropy == last_entropy)
                break;

            // Have to keep partial_key_positions sorted.
            partial_key_positions.insert(
                std::upper_bound(partial_key_positions.begin(), partial_key_positions.end(), new_position), new_position);
            last_entropy = new_entropy;
        }
        return partial_key_positions;
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Username {} not registered in entropy learned hashing", user_name);
            auto it_id = it_user->second.find(id);
            if (it_id == it_user->second.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Id {} not registered for user {} in entropy learned hashing", id, user_name);
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
    explicit FunctionTrainEntropyLearnedHash(const String & user_name_) : IFunction(), user_name(user_name_) { }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{{"data", &isString<IDataType>, nullptr, "String"}};
        FunctionArgumentDescriptors optional_args{{"id", &isString<IDataType>, nullptr, "String"}};

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto * data_col = arguments[0].column.get();
        if (const ColumnString * col_data_string = checkAndGetColumn<ColumnString>(data_col))
        {
            const size_t num_rows = col_data_string->size();

            std::vector<std::string_view> training_data;
            for (size_t i = 0; i < num_rows; ++i)
            {
                std::string_view string_view = col_data_string->getDataAt(i).toView();
                training_data.emplace_back(string_view);
            }

            // chooseBytes returns sorted vector
            PartialKeyPositions partial_key_positions = chooseBytes(training_data);

            if (arguments.size() == 2)
            {
                const IColumn * id_col = arguments[1].column.get();
                const ColumnConst * id_col_const = checkAndGetColumn<ColumnConst>(id_col);
                const String id = id_col_const->getValue<String>();

                auto & id_manager = IdManager::instance();
                id_manager.setPartialKeyPositionsForId(user_name, id, partial_key_positions);
            }

            // Making a return value:
            auto col_string_to = ColumnString::create();

            auto & data = col_string_to->getChars();
            auto & offsets = col_string_to->getOffsets();

            if (arguments.size() == 1)
            {
                // If trainEntropyLearnedHash('column') called, return a serialized bitmask with the special symbol '!' in the beginning
                data.push_back('!');
            }
            if (!partial_key_positions.empty())
                data.reserve(data.size() + *partial_key_positions.rbegin() + 2);

            size_t cur_position = 0;
            for (auto position : partial_key_positions)
            {
                while (cur_position < position)
                {
                    data.push_back('0');
                    ++cur_position;
                }
                data.push_back('1');
                ++cur_position;
            }
            data.push_back(0);

            offsets.push_back(data.size());

            return ColumnConst::create(std::move(col_string_to), num_rows);
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments.begin()->column->getName(),
                getName());
    }

private:
    const String user_name;
};


class FunctionEntropyLearnedHash : public IFunction
{
public:
    static constexpr auto name = "entropyLearnedHash";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionEntropyLearnedHash>(context->getUserName()); }
    explicit FunctionEntropyLearnedHash(const String & user_name_) : IFunction(), user_name(user_name_) { }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }


    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"data", &isString<IDataType>, nullptr, "String"},
            {"id", &isString<IDataType>, nullptr, "String"},
        };
        FunctionArgumentDescriptors optional_args{{"arbitraryHashFunction", &isString<IDataType>, nullptr, "String"}};

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        std::function<uint64_t(const char * s, size_t len)> hash_function;
        const auto arg_count = arguments.size();

        // Choosing arbitrary hash functions:
        if (arg_count == 2)
            hash_function = CityHash_v1_0_2::CityHash64;
        else
        {
            const IColumn * hash_function_col = arguments[2].column.get();
            const ColumnConst * hash_function_col_const = checkAndGetColumn<ColumnConst>(hash_function_col);
            String hash_function_name = hash_function_col_const->getValue<String>();
            String lowercase_name = hash_function_name;
            std::transform(
                hash_function_name.begin(),
                hash_function_name.end(),
                lowercase_name.begin(),
                [](unsigned char c) { return std::tolower(c); });
            if (lowercase_name == "cityhash64")
                hash_function = CityHash_v1_0_2::CityHash64;
            else if (lowercase_name == "farmhash64")
                hash_function = [](const char * s, size_t len) -> uint64_t
                { return static_cast<uint64_t>(NAMESPACE_FOR_HASH_FUNCTIONS::Hash64(s, len)); };
            else if (lowercase_name == "siphash64")
                hash_function = [](const char * s, size_t len) -> uint64_t { return sipHash64(s, len); };
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Hash function {} is not supported", hash_function_name);
        }

        const IColumn * id_or_bitmask_col = arguments[1].column.get();
        const ColumnConst * id_or_bitmask_col_const = checkAndGetColumn<ColumnConst>(id_or_bitmask_col);
        const String id_or_bitmask = id_or_bitmask_col_const->getValue<String>();

        PartialKeyPositions partial_key_positions;
        // If id_or_bitmask == '!', it is bitmask from call trainEntropyLearnedHash('column')
        if (!id_or_bitmask.empty() && id_or_bitmask[0] != '!')
        {
            const auto & id_manager = IdManager::instance();
            partial_key_positions = id_manager.getPartialKeyPositionsForId(user_name, id_or_bitmask);
        }
        else
            for (size_t i = 1; i < id_or_bitmask.size(); ++i)
                if (id_or_bitmask[i] == '1')
                    partial_key_positions.push_back(i - 1);

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
                col_res_vec[i] = hash_function(partial_key.data(), partial_key.size());
            }

            return col_res;
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments.begin()->column->getName(),
                getName());
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
