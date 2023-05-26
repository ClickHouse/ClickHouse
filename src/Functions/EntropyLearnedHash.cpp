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
    extern const int SUPPORT_IS_DISABLED;
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

/// Contains global state to convey information between SQL functions
/// - prepareTrainEntropyLearnedHash(),
/// - trainEntropyLearnedHash() and
/// - entropyLearnedHash().
///
/// The reason this machinery is necessary is that ClickHouse processes data in chunks of unpredictable size, yet the training step of
/// entropy-learned hashing needs to process *all* training data in one go. The downside is that the training step becomes quite expensive :-(
class EntropyLearnedHashGlobalState
{
public:
    static EntropyLearnedHashGlobalState & instance()
    {
        static EntropyLearnedHashGlobalState instance;
        return instance;
    }

    /// Called by prepareTrainEntropyLearnedHash()
    void cacheTrainingSample(const String & user_name, const String & id, IColumn::MutablePtr column)
    {
        std::lock_guard lock(mutex);
        auto & ids_for_user = global_state[user_name];
        auto & training_samples_for_id = ids_for_user[id].training_samples;
        training_samples_for_id.push_back(std::move(column));
    }

    void train(const String & user_name, const String & id)
    {
        std::lock_guard lock(mutex);
        auto & ids_for_user = global_state[user_name];
        auto & training_samples = ids_for_user[id].training_samples;

        if (training_samples.empty())
            return;

        auto & concatenated_training_sample = training_samples[0];
        for (size_t i = 1; i < training_samples.size(); ++i)
        {
            auto & other_training_sample = training_samples[i];
            concatenated_training_sample->insertRangeFrom(*other_training_sample, 0, other_training_sample->size());
        }

        const ColumnString * concatenated_training_sample_string = checkAndGetColumn<ColumnString>(*concatenated_training_sample);
        if (!concatenated_training_sample_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column");

        const size_t num_rows = concatenated_training_sample_string->size();
        std::vector<std::string_view> training_data;
        for (size_t i = 0; i < num_rows; ++i)
        {
            std::string_view string_view = concatenated_training_sample_string->getDataAt(i).toView();
            training_data.emplace_back(string_view);
        }

        PartialKeyPositions partial_key_positions = chooseBytes(training_data).first;

        ids_for_user[id].partial_key_positions = partial_key_positions;
        training_samples.clear();
    }

    const PartialKeyPositions & getPartialKeyPositions(const String & user_name, const String & id) const
    {
        std::lock_guard lock(mutex);
        auto it_user = global_state.find(user_name);
        if (it_user == global_state.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Id {} not registered for user in entropy learned hashing", id);
        auto it_id = it_user->second.find(id);
        if (it_id == it_user->second.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Id {} not registered for user in entropy learned hashing", id);
        return it_id->second.partial_key_positions;
    }

private:
    mutable std::mutex mutex;

    /// The state.
    struct ColumnsAndPartialKeyPositions
    {
        /// Caches training data chunks. Filled by prepareTrainEntropyLearnedHash(), cleared by trainEntropyLearnedHash().
        MutableColumns training_samples;
        /// The result of the training phase. Filled by trainEntropyLearnedHash().
        PartialKeyPositions partial_key_positions;
    };

    /// Maps a state id to the state.
    using IdToColumnsAndPartialKeyPositions = std::map<String, ColumnsAndPartialKeyPositions>;

    /// Maps the user name to a state id. As a result, the state id is unique at user scope.
    using UserNameToId = std::map<String, IdToColumnsAndPartialKeyPositions>;

    UserNameToId global_state TSA_GUARDED_BY(mutex);
};

}


/// Copies all chunks of the training sample column into the global state under a given id.
class FunctionPrepareTrainEntropyLearnedHash : public IFunction
{
public:
    static constexpr auto name = "prepareTrainEntropyLearnedHash";
    static FunctionPtr create(ContextPtr context)
    {
        if(!context->getSettings().allow_experimental_hash_functions)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Entropy-learned hashing is experimental. Set `allow_experimental_hash_functions` setting to enable it");

        return std::make_shared<FunctionPrepareTrainEntropyLearnedHash>(context->getUserName());
    }
    explicit FunctionPrepareTrainEntropyLearnedHash(const String & user_name_) : IFunction(), user_name(user_name_) {}

    String getName() const override { return name; }
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

        IColumn::Ptr data_col = arguments[0].column;
        IColumn::MutablePtr data_col_mutable = IColumn::mutate(data_col);

        auto & global_state = EntropyLearnedHashGlobalState::instance();
        global_state.cacheTrainingSample(user_name, id, std::move(data_col_mutable));

        const size_t num_rows = data_col->size();
        return result_type->createColumnConst(num_rows, 0u); /// dummy output
    }
private:
    const String user_name;
};


/// 1. Concatenates the training samples of a given id in the global state.
/// 2. Computes the partial key positions from the concatenated training samples and stores that in the global state.
/// 3. clear()-s the training samples in the global state.
class FunctionTrainEntropyLearnedHash : public IFunction
{
public:
    static constexpr auto name = "trainEntropyLearnedHash";
    static FunctionPtr create(ContextPtr context)
    {
        if(!context->getSettings().allow_experimental_hash_functions)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Entropy-learned hashing is experimental. Set `allow_experimental_hash_functions` setting to enable it");
        return std::make_shared<FunctionTrainEntropyLearnedHash>(context->getUserName());
    }
    explicit FunctionTrainEntropyLearnedHash(const String & user_name_) : IFunction(), user_name(user_name_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"id", &isString<IDataType>, nullptr, "String"}
        };

        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        const IColumn * id_col = arguments[0].column.get();
        const ColumnConst * id_col_const = checkAndGetColumn<ColumnConst>(id_col);
        if (!id_col_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments.begin()->column->getName(), getName());

        auto & global_state = EntropyLearnedHashGlobalState::instance();

        const String id = id_col_const->getValue<String>();
        global_state.train(user_name, id);

        const size_t num_rows = id_col->size();
        return result_type->createColumnConst(num_rows, 0u); /// dummy output
    }
private:
    const String user_name;
};


/// Hashes input strings using partial key positions stored in the global state.
class FunctionEntropyLearnedHash : public IFunction
{
public:
    static constexpr auto name = "entropyLearnedHash";
    static FunctionPtr create(ContextPtr context)
    {
        if(!context->getSettings().allow_experimental_hash_functions)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Entropy-learned hashing experimental. Set `allow_experimental_hash_functions` setting to enable it");
        return std::make_shared<FunctionEntropyLearnedHash>(context->getUserName());
    }
    explicit FunctionEntropyLearnedHash(const String & user_name_) : IFunction(), user_name(user_name_) {}

    String getName() const override { return name; }
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

        const auto & global_state = EntropyLearnedHashGlobalState::instance();
        const auto & partial_key_positions = global_state.getPartialKeyPositions(user_name, id);

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
    factory.registerFunction<FunctionPrepareTrainEntropyLearnedHash>();
    factory.registerFunction<FunctionTrainEntropyLearnedHash>();
    factory.registerFunction<FunctionEntropyLearnedHash>();
}

}
