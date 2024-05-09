#include <cmath>
#include <memory>
#include <string>
#include <unordered_map>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include "Common/Logger.h"
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB {

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionSerial : public IFunction
{
private:
    mutable zkutil::ZooKeeperPtr zk{nullptr};
    ContextPtr context;

public:
    static constexpr auto name = "serial";

    explicit FunctionSerial(ContextPtr ctx) : context(ctx)
    {
        if (ctx->hasZooKeeper()) {
            zk = ctx->getZooKeeper();
        }
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionSerial>(std::move(context));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override { return true; }
    bool hasInformationAboutMonotonicity() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1.",
                getName(), arguments.size());
        if (!isStringOrFixedString(arguments[0])) {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Type of argument for function {} doesn't match: passed {}, should be string",
                getName(), arguments[0]->getName());
        }

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<Int64>::create();
        typename ColumnVector<Int64>::Container & vec_to = col_res->getData();
        size_t size = input_rows_count;
        LOG_INFO(getLogger("Serial Function"), "Size = {}", size);
        vec_to.resize(size);

        const auto & serial_path = "/serials/" + arguments[0].column->getDataAt(0).toString();

        // if serial name used first time
        zk->createAncestors(serial_path);
        zk->createIfNotExists(serial_path, "");

        Int64 counter;

        if (zk != nullptr) {
            // Get Lock in ZooKeeper
            // https://zookeeper.apache.org/doc/r3.2.2/recipes.html

            // 1.
            if (zk->expired()) {
                zk = context->getZooKeeper();
            }

            std::string lock_path = serial_path + "/lock-";
            std::string path_created = zk->create(lock_path, "", zkutil::CreateMode::EphemeralSequential);
            Int64 created_sequence_number = std::stoll(path_created.substr(lock_path.size(), path_created.size() - lock_path.size()));

            while (true) {
                // 2.
                zkutil::Strings children = zk->getChildren(serial_path);

                // 3.
                Int64 lowest_child_sequence_number = -1;
                for (auto& child : children) {
                    if (child == "counter") {
                        continue;
                    }
                    std::string child_suffix = child.substr(5, 10);
                    Int64 seq_number = std::stoll(child_suffix);

                    if (lowest_child_sequence_number == -1 || seq_number < lowest_child_sequence_number) {
                        lowest_child_sequence_number = seq_number;
                    }
                }

                if (lowest_child_sequence_number == created_sequence_number) {
                    break;
                    // we have a lock in ZooKeeper, now can get the counter value
                }

                // 4. and 5.
                Int64 prev_seq_number = created_sequence_number - 1;
                std::string to_wait_key = std::to_string(prev_seq_number);
                while (to_wait_key.size() != 10) {
                    to_wait_key = "0" + to_wait_key;
                }

                zk->waitForDisappear(lock_path + to_wait_key);
            }

            // Now we have a lock
            // Update counter in ZooKeeper
            std::string counter_path = serial_path + "/counter";
            if (zk->exists(counter_path)) {
                std::string counter_string = zk->get(counter_path, nullptr);
                counter = std::stoll(counter_string);

                LOG_INFO(getLogger("Serial Function"), "Got counter from Zookeeper = {}", counter);
            } else {
                counter = 1;
            }
            zk->createOrUpdate(counter_path, std::to_string(counter + input_rows_count), zkutil::CreateMode::Persistent);

            // Unlock = delete node created on step 1.
            zk->deleteEphemeralNodeIfContentMatches(path_created, "");
        } else {
            // ZooKeeper is not available
            // What to do?

            counter = 1;
        }

        // Make a result
        for (auto& val : vec_to) {
            val = counter;
            ++counter;
        }


        return col_res;
    }

};

REGISTER_FUNCTION(Serial)
{
    factory.registerFunction<FunctionSerial>();
}

}
