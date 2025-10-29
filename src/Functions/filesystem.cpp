#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IDisk.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_DISK;
}
namespace
{

struct FilesystemAvailable
{
    static constexpr auto name = "filesystemAvailable";
    static UInt64 get(const DiskPtr & disk) { return disk->getAvailableSpace().value_or(std::numeric_limits<UInt64>::max()); }
};

struct FilesystemUnreserved
{
    static constexpr auto name = "filesystemUnreserved";
    static UInt64 get(const DiskPtr & disk) { return disk->getUnreservedSpace().value_or(std::numeric_limits<UInt64>::max()); }
};

struct FilesystemCapacity
{
    static constexpr auto name = "filesystemCapacity";
    static UInt64 get(const DiskPtr & disk) { return disk->getTotalSpace().value_or(std::numeric_limits<UInt64>::max()); }
};

template <typename Impl>
class FilesystemImpl : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FilesystemImpl<Impl>>(context_); }

    explicit FilesystemImpl(ContextPtr context_) : context(context_) { }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 0 or 1", getName());
        }
        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0]))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be String or FixedString",
                getName());
        }
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.empty())
        {
            auto disk = context->getDisk("default");
            return DataTypeUInt64().createColumnConst(input_rows_count, Impl::get(disk));
        }

        auto col = arguments[0].column;
        if (const ColumnString * col_str = checkAndGetColumn<ColumnString>(col.get()))
        {
            auto disk_map = context->getDisksMap();

            auto col_res = ColumnVector<UInt64>::create(col_str->size());
            auto & data = col_res->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto disk_name = col_str->getDataAt(i).toString();
                if (auto it = disk_map.find(disk_name); it != disk_map.end())
                    data[i] = Impl::get(it->second);
                else
                    throw Exception(ErrorCodes::UNKNOWN_DISK, "Unknown disk name {} while execute function {}", disk_name, getName());
            }
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(Filesystem)
{
    FunctionDocumentation::Description description_filesystemAvailable = R"(
Returns the amount of free space in the filesystem hosting the database persistence.
The returned value is always smaller than the total free space ([`filesystemUnreserved`](../../sql-reference/functions/other-functions.md#filesystemUnreserved)) because some space is reserved for the operating system.
    )";
    FunctionDocumentation::Syntax syntax_filesystemAvailable = "filesystemAvailable([disk_name])";
    FunctionDocumentation::Arguments arguments_filesystemAvailable = {
        {"disk_name", "Optional. The disk name to find the amount of free space for. If omitted, uses the default disk.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_filesystemAvailable = {"Returns the amount of remaining space available in bytes.", {"UInt64"}};
    FunctionDocumentation::Examples examples_filesystemAvailable = {
    {
        "Usage example",
        R"(
SELECT formatReadableSize(filesystemAvailable()) AS "Available space";
        )",
        R"(
┌─Available space─┐
│ 30.75 GiB       │
└─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_filesystemAvailable = {20, 1};
    FunctionDocumentation::Category category_filesystemAvailable = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_filesystemAvailable = {description_filesystemAvailable, syntax_filesystemAvailable, arguments_filesystemAvailable, returned_value_filesystemAvailable, examples_filesystemAvailable, introduced_in_filesystemAvailable, category_filesystemAvailable};

    factory.registerFunction<FilesystemImpl<FilesystemAvailable>>(documentation_filesystemAvailable);

    FunctionDocumentation::Description description_filesystemCapacity = R"(
Returns the capacity of the filesystem in bytes.
Needs the [path](../../operations/server-configuration-parameters/settings.md#path) to the data directory to be configured.
)";
    FunctionDocumentation::Syntax syntax_filesystemCapacity = "filesystemCapacity([disk_name])";
    FunctionDocumentation::Arguments arguments_filesystemCapacity = {
        {"disk_name", "Optional. The disk name to get the capacity for. If omitted, uses the default disk.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_filesystemCapacity = {"Returns the capacity of the filesystem in bytes.", {"UInt64"}};
    FunctionDocumentation::Examples examples_filesystemCapacity = {
    {
        "Usage example",
        R"(
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity";
        )",
        R"(
┌─Capacity──┐
│ 39.32 GiB │
└───────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_filesystemCapacity = {20, 1};
    FunctionDocumentation::Category category_filesystemCapacity = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_filesystemCapacity = {description_filesystemCapacity, syntax_filesystemCapacity, arguments_filesystemCapacity, returned_value_filesystemCapacity, examples_filesystemCapacity, introduced_in_filesystemCapacity, category_filesystemCapacity};

    factory.registerFunction<FilesystemImpl<FilesystemCapacity>>(documentation_filesystemCapacity);

    FunctionDocumentation::Description description_filesystemUnreserved = R"(
Returns the total amount of free space on the filesystem hosting the database persistence (previously `filesystemFree`).
See also [`filesystemAvailable`](#filesystemAvailable).
)";
    FunctionDocumentation::Syntax syntax_filesystemUnreserved = "filesystemUnreserved([disk_name])";
    FunctionDocumentation::Arguments arguments_filesystemUnreserved = {
        {"disk_name", "Optional. The disk name for which to find the total amount of free space. If omitted, uses the default disk.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_filesystemUnreserved = {"Returns the amount of free space in bytes.", {"UInt64"}};
    FunctionDocumentation::Examples examples_filesystemUnreserved = {
    {
        "Usage example",
        R"(
SELECT formatReadableSize(filesystemUnreserved()) AS "Free space";
        )",
        R"(
┌─Free space─┐
│ 32.39 GiB  │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_filesystemUnreserved = {22, 12};
    FunctionDocumentation::Category category_filesystemUnreserved = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_filesystemUnreserved = {description_filesystemUnreserved, syntax_filesystemUnreserved, arguments_filesystemUnreserved, returned_value_filesystemUnreserved, examples_filesystemUnreserved, introduced_in_filesystemUnreserved, category_filesystemUnreserved};

    factory.registerFunction<FilesystemImpl<FilesystemUnreserved>>(documentation_filesystemUnreserved);
}

}
