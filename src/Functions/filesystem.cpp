#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <filesystem>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace
{

struct FilesystemAvailable
{
    static constexpr auto name = "filesystemAvailable";
    static std::uintmax_t get(const std::filesystem::space_info & spaceinfo) { return spaceinfo.available; }
};

struct FilesystemFree
{
    static constexpr auto name = "filesystemFree";
    static std::uintmax_t get(const std::filesystem::space_info & spaceinfo) { return spaceinfo.free; }
};

struct FilesystemCapacity
{
    static constexpr auto name = "filesystemCapacity";
    static std::uintmax_t get(const std::filesystem::space_info & spaceinfo) { return spaceinfo.capacity; }
};

template <typename Impl>
class FilesystemImpl : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FilesystemImpl<Impl>>(std::filesystem::space(context->getConfigRef().getString("path")));
    }

    explicit FilesystemImpl(std::filesystem::space_info spaceinfo_) : spaceinfo(spaceinfo_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt64().createColumnConst(input_rows_count, static_cast<UInt64>(Impl::get(spaceinfo)));
    }

private:
    std::filesystem::space_info spaceinfo;
};

}

void registerFunctionFilesystem(FunctionFactory & factory)
{
    factory.registerFunction<FilesystemImpl<FilesystemAvailable>>();
    factory.registerFunction<FilesystemImpl<FilesystemCapacity>>();
    factory.registerFunction<FilesystemImpl<FilesystemFree>>();
}

}
