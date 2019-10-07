#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <boost/filesystem.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

struct FilesystemAvailable
{
    static constexpr auto name = "filesystemAvailable";
    static boost::uintmax_t get(boost::filesystem::space_info & spaceinfo) { return spaceinfo.available; }
};

struct FilesystemFree
{
    static constexpr auto name = "filesystemFree";
    static boost::uintmax_t get(boost::filesystem::space_info & spaceinfo) { return spaceinfo.free; }
};

struct FilesystemCapacity
{
    static constexpr auto name = "filesystemCapacity";
    static boost::uintmax_t get(boost::filesystem::space_info & spaceinfo) { return spaceinfo.capacity; }
};

template <typename Impl>
class FilesystemImpl : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FilesystemImpl<Impl>>(boost::filesystem::space(context.getConfigRef().getString("path")));
    }

    explicit FilesystemImpl(boost::filesystem::space_info spaceinfo_) : spaceinfo(spaceinfo_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt64().createColumnConst(input_rows_count, static_cast<UInt64>(Impl::get(spaceinfo)));
    }

private:
    boost::filesystem::space_info spaceinfo;
};


void registerFunctionFilesystem(FunctionFactory & factory)
{
    factory.registerFunction<FilesystemImpl<FilesystemAvailable>>();
    factory.registerFunction<FilesystemImpl<FilesystemCapacity>>();
    factory.registerFunction<FilesystemImpl<FilesystemFree>>();
}

}
